with src as (
    select * from {{ source('raw', 'marketing') }}
),
typed as (
    select
        try_cast(date as date) as spend_date,
        nullif(trim(channel), '') as channel,
        try_cast(spend_pkr as double) as spend_pkr,
        try_cast(clicks as bigint) as clicks,
        try_cast(conversions as bigint) as conversions
    from src
),
clean as (
    select
        spend_date,
        channel,
        coalesce(spend_pkr, 0.0) as spend_pkr,
        coalesce(clicks, 0) as clicks,
        coalesce(conversions, 0) as conversions
    from typed
    where spend_date is not null
)
select * from clean
