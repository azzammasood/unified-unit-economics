with src as (
    select * from {{ source('raw', 'sales') }}
),
typed as (
    select
        try_cast(order_id as bigint) as order_id,
        try_cast(date as date) as order_date,
        try_cast(customer_id as bigint) as customer_id,
        nullif(trim(vertical), '') as vertical,
        try_cast(revenue_pkr as double) as revenue_pkr,
        try_cast(discount_amount as double) as discount_amount
    from src
),
deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by order_id
                order by order_date desc nulls last, revenue_pkr desc nulls last
            ) as _rn
        from typed
        where order_id is not null
    ) t
    where _rn = 1
)
select
    order_id,
    order_date,
    customer_id,
    vertical,
    revenue_pkr,
    coalesce(discount_amount, 0.0) as discount_amount
from deduped
