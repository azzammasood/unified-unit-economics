---
title: Unit Economics Workbook
---

This workbook is designed for analysts: filter, compare cohorts, and drill into outliers.

> All metrics are computed from **order-level contribution** in `analytics.marts_fct_unit_economics`.

```sql filtered_base
select *
from analytics.marts_fct_unit_economics
```

```sql kpis
select
  count(*) as orders,
  round(sum(revenue_pkr), 0) as revenue_pkr,
  round(sum(net_profit_pkr), 0) as net_profit_pkr,
  round(100 * avg(net_margin_pct), 2) as avg_margin_pct,
  round(100.0 * sum(fuel_consumed_liters) / nullif(sum(delivery_distance_km), 0), 3) as fuel_l_per_100_km
from (${filtered_base}) b
```

<Value data={kpis} value=orders title="Orders" />
<Value data={kpis} value=revenue_pkr title="Revenue (PKR)" />
<Value data={kpis} value=net_profit_pkr title="Net Profit (PKR)" />
<Value data={kpis} value=avg_margin_pct title="Avg Margin (%)" />
<Value data={kpis} value=fuel_l_per_100_km title="Fuel (L/100km)" />

## Contribution by Vertical

```sql margin_by_vertical
select
  vertical,
  count(*) as orders,
  round(sum(net_profit_pkr), 0) as net_profit_pkr,
  round(sum(revenue_pkr), 0) as revenue_pkr,
  round(100 * avg(net_margin_pct), 2) as avg_margin_pct,
  round(100.0 * sum(fuel_consumed_liters) / nullif(count(*), 0), 3) as fuel_l_per_order
from (${filtered_base}) b
group by 1
order by net_profit_pkr desc
```

<Table data={margin_by_vertical} title="Vertical Rollup (Filtered)" />

## Operational Efficiency

Fuel is the most variable logistics cost driver. Track **fuel liters per 100 deliveries** over time, and drill into the worst days.

```sql fuel_efficiency
select
  order_date,
  count(*) as deliveries,
  100.0 * sum(fuel_consumed_liters) / nullif(count(*), 0) as fuel_liters_per_100_deliveries,
  round(sum(net_profit_pkr), 0) as net_profit_pkr
from (${filtered_base}) b
group by 1
order by 1
```

<LineChart
  data={fuel_efficiency}
  x=order_date
  y=fuel_liters_per_100_deliveries
  title="Fuel Liters per 100 Deliveries (Over Time)"
/>

```sql worst_fuel_days
select *
from (${fuel_efficiency}) f
order by fuel_liters_per_100_deliveries desc
limit 10
```

<Table data={worst_fuel_days} title="Worst Fuel Efficiency Days" />

## Marketing Efficiency

Marketing efficiency is modeled as a **daily CAC** (spend divided by conversions). Use this panel to compare channel CAC vs revenue on channel-active days.

```sql channel_efficiency
with m as (
  select
    spend_date as order_date,
    channel,
    sum(spend_pkr) as spend_pkr,
    sum(conversions) as conversions,
    case when sum(conversions) > 0 then sum(spend_pkr) / sum(conversions) else null end as cac_pkr
  from analytics.staging_stg_marketing
  group by 1, 2
),
rev as (
  select
    order_date,
    sum(revenue_pkr) as revenue_pkr
  from (${filtered_base}) b
  group by 1
)
select
  m.channel,
  round(sum(m.spend_pkr), 0) as spend_pkr,
  round(sum(m.conversions), 0) as conversions,
  round(avg(m.cac_pkr), 2) as avg_cac_pkr,
  round(sum(r.revenue_pkr), 0) as revenue_pkr
from m
left join rev r using (order_date)
group by 1
order by revenue_pkr desc
```

<BarChart data={channel_efficiency} x=channel y=avg_cac_pkr title="Average CAC by Channel (PKR)" />
<BarChart data={channel_efficiency} x=channel y=revenue_pkr title="Revenue on Channel-Active Days (PKR)" />

## Drilldowns

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
from (${filtered_base}) b
order by net_profit_pkr asc
limit 50
```

<Table data={worst_orders} title="Worst Orders (by Net Profit)" />

