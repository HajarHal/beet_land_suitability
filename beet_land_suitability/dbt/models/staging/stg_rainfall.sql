with source as (
    select
        field_id,
        parse_date("%Y-%m-%d", date) as date,
        rainfall
    from {{ source("gee_raw_data", "rainfall_raw") }}
)
select *
from source
