{{ config (materialized='view') }}

with assets as (
    select * from {{ source('weekly_cash_balances', 'assets') }}
),

account_suppliers as (
    select * from {{ source ('weekly_cash_balances', 'account_suppliers') }}
),


currencies as (
    select * from {{ source ('weekly_cash_balances', 'currencies') }}
)

select
    asu.name as account_provider,
    a.id as account_id,
    c.name as currency
from assets a
left join currencies c on a.currency_id = c.id
left join account_suppliers asu on a.supplier_id = asu.id