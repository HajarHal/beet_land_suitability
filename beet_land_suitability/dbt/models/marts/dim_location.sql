with soil as (
    select
        field_id,
        lat,
        lon,
        soil_ph,
        soil_cec,
        clay,
        silt,
        sand
    from {{ ref("stg_soil") }}
)
select
    field_id,
    lat as latitude,
    lon as longitude,
    soil_ph,
    soil_cec,
    clay,
    silt,
    sand
from soil
