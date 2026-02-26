with source as (
    select
        field_id,
        parse_date("%Y-%m-%d", date) as date,
        ndvi
    from {{ source("gee_raw_data", "ndvi_raw") }}
)
select *
from source
