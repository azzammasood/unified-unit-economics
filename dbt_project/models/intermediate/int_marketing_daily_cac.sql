with m as (
    select * from {{ ref('stg_marketing') }}
),
by_date_channel as (
    select
        spend_date as order_date,
        channel,
        sum(spend_pkr) as spend_pkr,
        sum(conversions) as conversions
    from m
    group by 1, 2
),
with_cac as (
    select
        order_date,
        channel,
        spend_pkr,
        conversions,
        case
            when conversions > 0 then spend_pkr / conversions
            else null
        end as cac_pkr
    from by_date_channel
),
weighted_daily as (
    select
        order_date,
        sum(spend_pkr) as total_spend_pkr,
        sum(conversions) as total_conversions,
        case
            when sum(conversions) > 0 then sum(spend_pkr) / sum(conversions)
            else null
        end as avg_cac_pkr
    from with_cac
    group by 1
)
select * from weighted_daily
