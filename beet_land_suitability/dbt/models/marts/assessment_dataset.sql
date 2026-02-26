with fact_suitability as (
    select *
    from {{ ref("fact_suitability") }}
),
aggregated_scores as (
    select
        field_id,
        agricultural_period,
        avg(ndvi) as avg_ndvi,
        avg(temperature) as avg_temperature,
        avg(rainfall) as avg_rainfall,
        avg(ndvi_score) as avg_ndvi_score,
        avg(temperature_score) as avg_temperature_score,
        avg(rainfall_score) as avg_rainfall_score,
        avg(total_suitability_score) as avg_total_suitability_score
    from fact_suitability
    group by field_id, agricultural_period
),
pivoted_periods as (
    select
        field_id,
        avg(case when agricultural_period = 'germination_early_growth' then avg_ndvi_score else null end) as ndvi_score_germination_early_growth,
        avg(case when agricultural_period = 'vegetative_growth' then avg_ndvi_score else null end) as ndvi_score_vegetative_growth,
        avg(case when agricultural_period = 'sugar_accumulation' then avg_ndvi_score else null end) as ndvi_score_sugar_accumulation,
        avg(case when agricultural_period = 'germination_early_growth' then avg_temperature_score else null end) as temp_score_germination_early_growth,
        avg(case when agricultural_period = 'vegetative_growth' then avg_temperature_score else null end) as temp_score_vegetative_growth,
        avg(case when agricultural_period = 'sugar_accumulation' then avg_temperature_score else null end) as temp_score_sugar_accumulation,
        avg(case when agricultural_period = 'germination_early_growth' then avg_rainfall_score else null end) as rainfall_score_germination_early_growth,
        avg(case when agricultural_period = 'vegetative_growth' then avg_rainfall_score else null end) as rainfall_score_vegetative_growth,
        avg(case when agricultural_period = 'sugar_accumulation' then avg_rainfall_score else null end) as rainfall_score_sugar_accumulation,
        avg(case when agricultural_period = 'germination_early_growth' then avg_total_suitability_score else null end) as total_score_germination_early_growth,
        avg(case when agricultural_period = 'vegetative_growth' then avg_total_suitability_score else null end) as total_score_vegetative_growth,
        avg(case when agricultural_period = 'sugar_accumulation' then avg_total_suitability_score else null end) as total_score_sugar_accumulation
    from aggregated_scores
    group by field_id
),
final_ml_dataset as (
    select
        pl.field_id,
        dl.latitude,
        dl.longitude,
        dl.soil_ph,
        dl.soil_cec,
        dl.clay,
        dl.silt,
        dl.sand,
        pl.ndvi_score_germination_early_growth,
        pl.ndvi_score_vegetative_growth,
        pl.ndvi_score_sugar_accumulation,
        pl.temp_score_germination_early_growth,
        pl.temp_score_vegetative_growth,
        pl.temp_score_sugar_accumulation,
        pl.rainfall_score_germination_early_growth,
        pl.rainfall_score_vegetative_growth,
        pl.rainfall_score_sugar_accumulation,
        pl.total_score_germination_early_growth,
        pl.total_score_vegetative_growth,
        pl.total_score_sugar_accumulation
    from pivoted_periods pl
    left join {{ ref("dim_location") }} dl
        on pl.field_id = dl.field_id
)
select *
from final_ml_dataset
