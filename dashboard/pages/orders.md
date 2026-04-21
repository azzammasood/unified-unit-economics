---
title: Orders Deep Dive (Sales + Logistics)
---

This page is designed for operational teams. It answers:

- Which verticals are profitable after logistics and CAC?
- Are we paying riders and burning fuel in line with revenue?
- Which orders are outliers (negative contribution)?

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
from analytics.marts_fct_unit_economics
group by 1
order by net_profit_pkr desc
```

<Table data={vertical_rollup} title="Unit Economics by Vertical" />

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
from analytics.marts_fct_unit_economics
order by net_profit_pkr asc
limit 25
```

<Table data={worst_orders} title="Most Negative Net Profit Orders" />

