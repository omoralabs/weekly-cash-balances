{{config (materialized='view')}}

with base_rates as (
    select * from {{ref('base_rates_data')}}
),


forward_rates as (
    select
        weeknum,
        base_currency,
        quote_currency,
        value
    from base_rates
),

inverse_rates as (
    select
        weeknum,
        quote_currency as base_currency,
        base_currency as quote_currency,
        1/value as value
    from base_rates
)

select * from forward_rates
union all
select * from inverse_rates
