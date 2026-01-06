{{config (materialized='view')}}

with asset_data as (
    select * from {{ ref ('asset_data') }}
),

asset_values as (
    select * from {{source('weekly_cash_balances', 'asset_values')}}
)

select
    av.week_start,
    ad.account_provider,
    ad.account_id,
    ad.currency,
    av.value
from asset_data ad
left join asset_values av on av.asset_id = ad.account_id
order by av.week_start, ad.account_id
