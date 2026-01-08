{{config(materialised='view')}}

with converted_cash_balances as (
    select * from {{ref ('converted_cash_balances') }}
)

select
    weeknum,
    reporting_currency,
    ROUND(
        sum(converted_value)
        ,2) as converted_cash_balances
from converted_cash_balances
group by weeknum, reporting_currency
order by weeknum
