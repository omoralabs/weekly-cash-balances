{{ config (materialized='view') }}

with exchange_rates as (
    select * from {{ source ('weekly_cash_balances', 'exchange_rates') }}
),

currency_pairs as (
    select * from {{ source ('weekly_cash_balances', 'currency_pairs') }}
),

currencies as (
    select * from {{ source ('weekly_cash_balances', 'currencies') }}
)



select
    EXTRACT(YEAR from er.week_start) * 100 + EXTRACT(WEEK from er.week_start) as weeknum,
    c1.name as base_currency,
    c2.name as quote_currency,
    er.value
from exchange_rates er
join currency_pairs cp on er.currency_pair_id = cp.id
join currencies c1 on cp.base_currency_id = c1.id
join currencies c2 on cp.quote_currency_id = c2.id
