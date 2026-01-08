{{config (materialized='view')}}

with currency_amounts as (
    select * from  {{ref('currency_amounts')}}
),

currencies as (
    select * from {{source('weekly_cash_balances', 'currencies')}}
),

reporting_rates as (
    select * from  {{ref('reporting_rates')}}
)

select
    ca.weeknum,
    ca.currency,
    ca.total_value as base_value,
    c.name as reporting_currency,
    COALESCE(rr.value, 1) as conversion_rate,
    ROUND (
        case
            when ca.currency = c.name then ca.total_value
            else ca.total_value * rr.value
        end, 2) as converted_value
from currencies c
cross join currency_amounts ca
left join reporting_rates rr
    on ca.weeknum = rr.weeknum
    and ca.currency = rr.base_currency
    and c.name = rr.quote_currency
order by ca.weeknum
