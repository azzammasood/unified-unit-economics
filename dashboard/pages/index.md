---
title: Unit Economics Executive Report
---

```sql total_profit
select
  round(sum(net_profit_pkr), 0) as total_net_profit_pkr
from analytics.marts_fct_unit_economics
```

```sql margin_by_vertical
select
  vertical,
  round(sum(net_profit_pkr), 0) as net_profit_pkr,
  round(100 * avg(net_margin_pct), 2) as avg_margin_pct
from analytics.marts_fct_unit_economics
group by 1
order by net_profit_pkr desc
```

## Executive Summary

The company operates across **Sales**, **Marketing**, and **Logistics**. This report translates day-to-day activity into one question: **are we generating net contribution after discounts, rider payout, fuel, and marketing acquisition?**

### Topline

<Value data={total_profit} value=total_net_profit_pkr title="Total Net Profit (PKR)" />

<Table data={margin_by_vertical} title="Average Margin % by Vertical" />

## Operational Efficiency

Fuel is the most variable logistics cost driver. To detect drift (routing issues, rider behavior, demand clustering), we track **fuel liters per 100 deliveries** over time.

```sql fuel_efficiency
select
  order_date,
  100.0 * sum(fuel_consumed_liters) / nullif(count(*), 0) as fuel_liters_per_100_deliveries
from analytics.marts_fct_unit_economics
group by 1
order by 1
```

<LineChart
  data={fuel_efficiency}
  x=order_date
  y=fuel_liters_per_100_deliveries
  title="Fuel Liters per 100 Deliveries (Over Time)"
/>

## Marketing Efficiency

Marketing efficiency is modeled as an **estimated CAC** (spend divided by conversions) and allocated to orders by date. We monitor whether acquisition economics are improving relative to delivered revenue.

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
  from analytics.marts_fct_unit_economics
  group by 1
)
select
  m.channel,
  round(avg(m.cac_pkr), 2) as avg_cac_pkr,
  round(sum(r.revenue_pkr), 0) as revenue_pkr
from m
left join rev r using (order_date)
group by 1
order by revenue_pkr desc
```

<BarChart
  data={channel_efficiency}
  x=channel
  y=avg_cac_pkr
  title="Average CAC by Channel (PKR)"
/>

<BarChart
  data={channel_efficiency}
  x=channel
  y=revenue_pkr
  title="Revenue by Channel-Active Days (PKR)"
/>

