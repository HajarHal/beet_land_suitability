with harmonized_data as (
    select distinct
        date,
        extract(year from date) as year,
        extract(month from date) as month,
        agricultural_period
    from {{ ref("int_harmonized_environmental_data") }}
)
select
    date,
    year,
    month,
    agricultural_period
from harmonized_data
order by date
