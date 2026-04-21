---
title: Orders Deep Dive (Sales + Logistics)
---

This page is designed for analysts to investigate distribution + outliers. It answers:

- Which verticals are profitable after logistics and CAC?
- Are we paying riders and burning fuel in line with revenue?
- Which orders are outliers (negative contribution)?

## Controls

<DateRange name=start_date defaultValue="2026-01-01" />
<DateRange name=end_date defaultValue="2026-03-31" />
<Dropdown
  name=vertical
  defaultValue="All"
  options={["All","Bakery","Grocery","Pharma"]} />
<NumberInput name=top_n defaultValue={25} title="Top N Outliers" />

```sql base
select *
from analytics.marts_fct_unit_economics
where order_date between date '${inputs.start_date}' and date '${inputs.end_date}'
  and (${inputs.vertical} = 'All' or vertical = '${inputs.vertical}')
```

```sql vertical_rollup
select
  vertical,
  count(*) as orders,
  round(sum(revenue_pkr), 0) as revenue_pkr,
  round(sum(discount_amount), 0) as discounts_pkr,
  round(sum(rider_payout_pkr), 0) as rider_payout_pkr,
  round(sum(fuel_cost_pkr), 0) as fuel_cost_pkr,
  round(sum(estimated_marketing_cac_pkr), 0) as marketing_cac_pkr,
  round(sum(net_profit_pkr), 0) as net_profit_pkr,
  round(100 * avg(net_margin_pct), 2) as avg_margin_pct
from (${base}) b
group by 1
order by net_profit_pkr desc
```

<Table data={vertical_rollup} title="Unit Economics by Vertical" />

```sql margin_hist
select
  case
    when net_margin_pct is null then 'null'
    when net_margin_pct < -0.50 then '< -50%'
    when net_margin_pct < -0.25 then '-50% to -25%'
    when net_margin_pct < 0 then '-25% to 0%'
    when net_margin_pct < 0.10 then '0% to 10%'
    when net_margin_pct < 0.25 then '10% to 25%'
    when net_margin_pct < 0.50 then '25% to 50%'
    else '>= 50%'
  end as bucket,
  count(*) as orders
from (${base}) b
group by 1
order by orders desc
```

<BarChart data={margin_hist} x=bucket y=orders title="Net Margin Distribution (Buckets)" />

```sql worst_orders
select
  order_date,
  order_id,
  vertical,
  revenue_pkr,
  discount_amount,
  rider_payout_pkr,
  fuel_consumed_liters,
  fuel_cost_pkr,
  estimated_marketing_cac_pkr,
  net_profit_pkr,
  round(100 * net_margin_pct, 2) as net_margin_pct
from (${base}) b
order by net_profit_pkr asc
limit ${inputs.top_n}
```

<Table data={worst_orders} title="Most Negative Net Profit Orders" />

## Outlier Panels

```sql fuel_outliers
select
  order_date,
  order_id,
  vertical,
  delivery_distance_km,
  fuel_consumed_liters,
  round(100 * fuel_liters_per_km, 3) as fuel_l_per_100m,
  fuel_cost_pkr,
  net_profit_pkr
from (${base}) b
order by fuel_liters_per_km desc
limit ${inputs.top_n}
```

<Table data={fuel_outliers} title="Highest Fuel Burn per km" />

```sql payout_outliers
select
  order_date,
  order_id,
  vertical,
  delivery_distance_km,
  rider_payout_pkr,
  round(rider_payout_pkr / nullif(delivery_distance_km, 0), 2) as payout_pkr_per_km,
  net_profit_pkr
from (${base}) b
order by payout_pkr_per_km desc nulls last
limit ${inputs.top_n}
```

<Table data={payout_outliers} title="Highest Rider Payout per km" />

