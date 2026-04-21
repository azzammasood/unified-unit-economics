from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Config:
    out_dir: Path = Path(__file__).resolve().parent / "output"
    n_orders: int = 5000
    start_date: str = "2026-01-01"
    end_date: str = "2026-03-31"
    motherduck_database: str | None = None
    motherduck_token: str | None = None


def _rng(seed: int = 42) -> np.random.Generator:
    return np.random.default_rng(seed)


def _date_series(rng: np.random.Generator, n: int, start: str, end: str) -> pd.Series:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    days = (end_ts - start_ts).days
    offsets = rng.integers(0, days + 1, size=n)
    return (start_ts + pd.to_timedelta(offsets, unit="D")).normalize()


def generate_sales_csv(cfg: Config, rng: np.random.Generator) -> Path:
    verticals = np.array(["Bakery", "Grocery", "Pharma"])
    n = cfg.n_orders

    order_id = np.arange(1, n + 1, dtype=np.int64)
    date = _date_series(rng, n, cfg.start_date, cfg.end_date)
    customer_id = rng.integers(1, 1500, size=n, dtype=np.int64)
    vertical = rng.choice(verticals, size=n, replace=True)

    base = rng.gamma(shape=2.2, scale=900.0, size=n)  # positive, long tail
    vertical_multiplier = np.select(
        [vertical == "Bakery", vertical == "Grocery", vertical == "Pharma"],
        [0.8, 1.0, 1.4],
        default=1.0,
    )
    revenue_pkr = np.round(base * vertical_multiplier, 2)

    discount_rate = np.clip(rng.normal(loc=0.06, scale=0.05, size=n), 0, 0.35)
    discount_amount = np.round(revenue_pkr * discount_rate, 2)

    df = pd.DataFrame(
        {
            "order_id": order_id,
            "date": date.astype("datetime64[ns]"),
            "customer_id": customer_id,
            "vertical": vertical,
            "revenue_pkr": revenue_pkr,
            "discount_amount": discount_amount,
        }
    )

    # Dirty data: duplicate some order_ids with slightly shifted values
    dup_n = max(1, int(0.02 * n))
    dup_idx = rng.choice(df.index.to_numpy(), size=dup_n, replace=False)
    dup = df.loc[dup_idx].copy()
    dup["revenue_pkr"] = np.round(dup["revenue_pkr"] * rng.uniform(0.9, 1.1, size=dup_n), 2)
    dup["discount_amount"] = np.round(
        dup["discount_amount"] * rng.uniform(0.85, 1.2, size=dup_n), 2
    )

    # Dirty data: some missing discounts and a few missing customer_ids
    null_disc_n = max(1, int(0.01 * n))
    null_cust_n = max(1, int(0.005 * n))
    df.loc[rng.choice(df.index, size=null_disc_n, replace=False), "discount_amount"] = np.nan
    df.loc[rng.choice(df.index, size=null_cust_n, replace=False), "customer_id"] = np.nan

    df = pd.concat([df, dup], ignore_index=True)

    out = cfg.out_dir / "sales.csv"
    df.to_csv(out, index=False)
    return out


def generate_marketing_csv(cfg: Config, rng: np.random.Generator) -> Path:
    channels = np.array(["Meta", "Google", "TikTok"])

    all_dates = pd.date_range(cfg.start_date, cfg.end_date, freq="D")
    rows = []

    for d in all_dates:
        for ch in channels:
            spend = float(np.round(rng.gamma(shape=2.0, scale=2500.0), 2))
            clicks = int(rng.poisson(lam=max(5.0, spend / 90.0)))
            conv_rate = {"Meta": 0.03, "Google": 0.045, "TikTok": 0.022}[ch]
            conversions = int(rng.binomial(n=max(1, clicks), p=conv_rate))

            rows.append(
                {
                    "date": pd.Timestamp(d).normalize(),
                    "channel": ch,
                    "spend_pkr": spend,
                    "clicks": clicks,
                    "conversions": conversions,
                }
            )

    df = pd.DataFrame(rows)

    # Dirty data: null conversions on a few days, and an outlier spend row
    null_n = max(1, int(0.01 * len(df)))
    df.loc[rng.choice(df.index, size=null_n, replace=False), "conversions"] = np.nan
    outlier_idx = int(rng.integers(0, len(df)))
    df.loc[outlier_idx, "spend_pkr"] = float(df.loc[outlier_idx, "spend_pkr"]) * 8

    out = cfg.out_dir / "marketing.csv"
    df.to_csv(out, index=False)
    return out


def generate_logistics_csv(cfg: Config, rng: np.random.Generator) -> Path:
    n = cfg.n_orders
    order_id = np.arange(1, n + 1, dtype=np.int64)

    delivery_distance_km = np.round(rng.gamma(shape=2.5, scale=2.2, size=n), 2)
    delivery_distance_km = np.clip(delivery_distance_km, 0.3, 60.0)

    # fuel roughly proportional to distance with noise; units: liters
    base_lpkm = rng.normal(loc=0.06, scale=0.015, size=n)  # liters per km
    base_lpkm = np.clip(base_lpkm, 0.02, 0.14)
    fuel_consumed_liters = np.round(delivery_distance_km * base_lpkm, 3)

    rider_payout_pkr = np.round(120 + delivery_distance_km * rng.normal(18, 2.5, size=n), 2)
    rider_payout_pkr = np.clip(rider_payout_pkr, 90, 3000)

    df = pd.DataFrame(
        {
            "order_id": order_id,
            "delivery_distance_km": delivery_distance_km,
            "fuel_consumed_liters": fuel_consumed_liters,
            "rider_payout_pkr": rider_payout_pkr,
        }
    )

    # Dirty data: null fuel_consumed_liters for some deliveries
    null_n = max(1, int(0.03 * n))
    df.loc[rng.choice(df.index, size=null_n, replace=False), "fuel_consumed_liters"] = np.nan

    # Dirty data: duplicate order_id rows (conflicting measurements)
    dup_n = max(1, int(0.02 * n))
    dup_idx = rng.choice(df.index.to_numpy(), size=dup_n, replace=False)
    dup = df.loc[dup_idx].copy()
    dup["delivery_distance_km"] = np.round(
        dup["delivery_distance_km"] * rng.uniform(0.9, 1.2, size=dup_n), 2
    )
    dup["fuel_consumed_liters"] = np.round(
        dup["fuel_consumed_liters"].fillna(0.0) * rng.uniform(0.85, 1.25, size=dup_n), 3
    )
    dup["rider_payout_pkr"] = np.round(
        dup["rider_payout_pkr"] * rng.uniform(0.9, 1.15, size=dup_n), 2
    )

    df = pd.concat([df, dup], ignore_index=True)

    out = cfg.out_dir / "logistics.csv"
    df.to_csv(out, index=False)
    return out


def _connect_motherduck(database: str, token: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL motherduck;")
    con.execute("LOAD motherduck;")
    con.execute(f"SET motherduck_token='{token}';")
    con.execute(f"ATTACH 'md:{database}' AS md (READ_ONLY FALSE);")
    con.execute("USE md;")
    return con


def _copy_csv_to_table(con: duckdb.DuckDBPyConnection, csv_path: Path, schema: str, table: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    con.execute(
        f"""
        CREATE TABLE {schema}.{table} AS
        SELECT *
        FROM read_csv_auto('{str(csv_path)}', HEADER=TRUE, ALL_VARCHAR=TRUE);
        """
    )


def main() -> None:
    cfg = Config(
        out_dir=Path(os.getenv("DATA_GEN_OUT_DIR", str(Config.out_dir))),
        n_orders=int(os.getenv("N_ORDERS", str(Config.n_orders))),
        start_date=os.getenv("START_DATE", Config.start_date),
        end_date=os.getenv("END_DATE", Config.end_date),
        motherduck_database=os.getenv("MOTHERDUCK_DATABASE"),
        motherduck_token=os.getenv("MOTHERDUCK_TOKEN"),
    )

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    rng = _rng(seed=int(os.getenv("DATA_GEN_SEED", "42")))

    sales_path = generate_sales_csv(cfg, rng)
    marketing_path = generate_marketing_csv(cfg, rng)
    logistics_path = generate_logistics_csv(cfg, rng)

    print(f"Wrote {sales_path}")
    print(f"Wrote {marketing_path}")
    print(f"Wrote {logistics_path}")

    # Optional: ingest to MotherDuck (fast path for CI)
    if cfg.motherduck_database and cfg.motherduck_token:
        con = _connect_motherduck(cfg.motherduck_database, cfg.motherduck_token)
        _copy_csv_to_table(con, sales_path, "raw", "sales")
        _copy_csv_to_table(con, marketing_path, "raw", "marketing")
        _copy_csv_to_table(con, logistics_path, "raw", "logistics")
        con.close()
        print("Ingested CSVs into MotherDuck schema raw.*")
    else:
        print("Skipping MotherDuck ingest (set MOTHERDUCK_DATABASE and MOTHERDUCK_TOKEN to enable).")


if __name__ == "__main__":
    main()
