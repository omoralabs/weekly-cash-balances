{{config (materialized='view')}}

with asset_data as (
    select * from {{ ref ('asset_data') }}
),

asset_values as (
    select * from {{source('weekly_cash_balances', 'asset_values')}}
)

select
    EXTRACT(YEAR from av.week_start) * 100 + EXTRACT(WEEK from av.week_start) as weeknum,
    ad.account_provider,
    ad.account_id,
    ad.currency,
    COALESCE(av.value,0) as value
from asset_data ad
left join asset_values av on av.asset_id = ad.account_id
order by av.week_start, ad.account_id
