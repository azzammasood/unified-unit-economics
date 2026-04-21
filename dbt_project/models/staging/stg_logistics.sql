with src as (
    select * from {{ source('raw', 'logistics') }}
),
typed as (
    select
        try_cast(order_id as bigint) as order_id,
        try_cast(delivery_distance_km as double) as delivery_distance_km,
        try_cast(fuel_consumed_liters as double) as fuel_consumed_liters,
        try_cast(rider_payout_pkr as double) as rider_payout_pkr
    from src
),
deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by order_id
                order by
                    fuel_consumed_liters desc nulls last,
                    delivery_distance_km desc nulls last,
                    rider_payout_pkr desc nulls last
            ) as _rn
        from typed
        where order_id is not null
    ) t
    where _rn = 1
),
standardized as (
    select
        order_id,
        coalesce(delivery_distance_km, 0.0) as delivery_distance_km,
        fuel_consumed_liters,
        coalesce(rider_payout_pkr, 0.0) as rider_payout_pkr,
        case
            when coalesce(delivery_distance_km, 0.0) > 0 and fuel_consumed_liters is not null
                then fuel_consumed_liters / delivery_distance_km
            else null
        end as fuel_liters_per_km
    from deduped
)
select
    order_id,
    delivery_distance_km,
    coalesce(fuel_consumed_liters, 0.0) as fuel_consumed_liters,
    rider_payout_pkr,
    coalesce(fuel_liters_per_km, 0.0) as fuel_liters_per_km
from standardized
