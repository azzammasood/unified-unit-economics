with o as (
    select * from {{ ref('stg_sales') }}
),
l as (
    select * from {{ ref('stg_logistics') }}
),
joined as (
    select
        o.order_id,
        o.order_date,
        o.customer_id,
        o.vertical,
        o.revenue_pkr,
        o.discount_amount,
        l.delivery_distance_km,
        l.fuel_consumed_liters,
        l.fuel_liters_per_km,
        l.rider_payout_pkr
    from o
    left join l using (order_id)
)
select * from joined
