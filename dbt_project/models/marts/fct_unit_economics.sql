with orders as (
    select * from {{ ref('int_orders_enriched') }}
),
daily_cac as (
    select * from {{ ref('int_marketing_daily_cac') }}
),
joined as (
    select
        o.order_id,
        o.order_date,
        o.customer_id,
        o.vertical,
        o.revenue_pkr,
        o.discount_amount,
        o.delivery_distance_km,
        o.fuel_consumed_liters,
        o.fuel_liters_per_km,
        o.rider_payout_pkr,
        coalesce(c.avg_cac_pkr, 0.0) as estimated_marketing_cac_pkr
    from orders o
    left join daily_cac c
        on c.order_date = o.order_date
),
final as (
    select
        *,
        275.0 as fuel_cost_pkr_per_liter,
        (fuel_consumed_liters * 275.0) as fuel_cost_pkr,
        (
            revenue_pkr
            - (
                discount_amount
                + rider_payout_pkr
                + (fuel_consumed_liters * 275.0)
                + estimated_marketing_cac_pkr
            )
        ) as net_profit_pkr,
        case
            when revenue_pkr is not null and revenue_pkr != 0
                then (
                    revenue_pkr
                    - (
                        discount_amount
                        + rider_payout_pkr
                        + (fuel_consumed_liters * 275.0)
                        + estimated_marketing_cac_pkr
                    )
                ) / revenue_pkr
            else null
        end as net_margin_pct
    from joined
)
select * from final
