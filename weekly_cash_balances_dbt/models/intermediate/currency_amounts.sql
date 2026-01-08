{{config(materialized='view')}}

with asset_values_data as (
    select * from {{ref('asset_values_data')}}
)

select
    weeknum,
    currency,
    sum(value) as total_value
from asset_values_data
group by weeknum, currency
order by weeknum
