with source as (
    select
        field_id,
        lat,
        lon,
        soil_ph,
        soil_cec,
        clay,
        silt,
        sand
    from {{ source("gee_raw_data", "soil_raw") }}
)
select *
from source
