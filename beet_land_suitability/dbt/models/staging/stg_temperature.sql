with source as (
    select
        field_id,
        parse_date("%Y-%m-%d", date) as date,
        temperature
    from {{ source("gee_raw_data", "temperature_raw") }}
)
select *
from source
