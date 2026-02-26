with ndvi as (
    select
        field_id,
        date,
        extract(month from date) as month,
        ndvi
    from {{ ref("stg_ndvi") }}
),
temperature as (
    select
        field_id,
        date,
        extract(month from date) as month,
        temperature
    from {{ ref("stg_temperature") }}
),
rainfall as (
    select
        field_id,
        date,
        extract(month from date) as month,
        rainfall
    from {{ ref("stg_rainfall") }}
),
soil as (
    select
        field_id,
        soil_ph,
        soil_cec,
        clay,
        silt,
        sand
    from {{ ref("stg_soil") }}
),
monthly_data as (
    select
        coalesce(n.field_id, t.field_id, r.field_id) as field_id,
        coalesce(n.date, t.date, r.date) as date,
        coalesce(n.month, t.month, r.month) as month,
        n.ndvi,
        t.temperature,
        r.rainfall,
        case
            when coalesce(n.month, t.month, r.month) in (11, 12, 1, 2) then 'germination_early_growth'
            when coalesce(n.month, t.month, r.month) in (3, 4, 5) then 'vegetative_growth'
            when coalesce(n.month, t.month, r.month) in (6, 7, 8) then 'sugar_accumulation'
            else 'other_period' -- Or handle as needed
        end as agricultural_period
    from ndvi n
    full outer join temperature t
        on n.field_id = t.field_id and n.date = t.date
    full outer join rainfall r
        on coalesce(n.field_id, t.field_id) = r.field_id and coalesce(n.date, t.date) = r.date
)
select
    md.field_id,
    md.date,
    md.month,
    md.agricultural_period,
    md.ndvi,
    md.temperature,
    md.rainfall,
    s.soil_ph,
    s.soil_cec,
    s.clay,
    s.silt,
    s.sand
from monthly_data md
left join soil s
    on md.field_id = s.field_id
