with harmonized_data as (
    select *
    from {{ ref("int_harmonized_environmental_data") }}
),
dim_thresholds as (
    select *
    from {{ ref("dim_thresholds") }}
),
calculate_scores as (
    select
        hd.field_id,
        hd.date,
        hd.agricultural_period,
        hd.ndvi,
        hd.temperature,
        hd.rainfall,
        hd.soil_ph,
        hd.soil_cec,
        hd.clay,
        hd.silt,
        hd.sand,
        case
            when hd.ndvi is null then 0.0
            when hd.ndvi between tn.optimal_min and tn.optimal_max then 1.0
            when hd.ndvi >= tn.acceptable_min and hd.ndvi < tn.optimal_min then
                (hd.ndvi - tn.acceptable_min) / (tn.optimal_min - tn.acceptable_min) * 0.5 + 0.5
            when hd.ndvi > tn.optimal_max and hd.ndvi <= tn.acceptable_max then
                1.0 - ((hd.ndvi - tn.optimal_max) / (tn.acceptable_max - tn.optimal_max) * 0.5)
            else 0.0
        end as ndvi_score,
        case
            when hd.temperature is null then 0.0
            when hd.temperature between tt.optimal_min and tt.optimal_max then 1.0
            when hd.temperature >= tt.acceptable_min and hd.temperature < tt.optimal_min then
                (hd.temperature - tt.acceptable_min) / (tt.optimal_min - tt.acceptable_min) * 0.5 + 0.5
            when hd.temperature > tt.optimal_max and hd.temperature <= tt.acceptable_max then
                1.0 - ((hd.temperature - tt.optimal_max) / (tt.acceptable_max - tt.optimal_max) * 0.5)
            else 0.0
        end as temperature_score,
        case
            when hd.rainfall is null then 0.0
            when hd.rainfall between tr.optimal_min and tr.optimal_max then 1.0
            when hd.rainfall >= tr.acceptable_min and hd.rainfall < tr.optimal_min then
                (hd.rainfall - tr.acceptable_min) / (tr.optimal_min - tr.acceptable_min) * 0.5 + 0.5
            when hd.rainfall > tr.optimal_max and hd.rainfall <= tr.acceptable_max then
                1.0 - ((hd.rainfall - tr.optimal_max) / (tr.acceptable_max - tr.optimal_max) * 0.5)
            else 0.0
        end as rainfall_score,
        case
            when hd.soil_ph is null then 0.0
            when hd.soil_ph between tsp.optimal_min and tsp.optimal_max then 1.0
            when hd.soil_ph >= tsp.acceptable_min and hd.soil_ph < tsp.optimal_min then
                (hd.soil_ph - tsp.acceptable_min) / (tsp.optimal_min - tsp.acceptable_min) * 0.5 + 0.5
            when hd.soil_ph > tsp.optimal_max and hd.soil_ph <= tsp.acceptable_max then
                1.0 - ((hd.soil_ph - tsp.optimal_max) / (tsp.acceptable_max - tsp.optimal_max) * 0.5)
            else 0.0
        end as soil_ph_score,
        case
            when hd.soil_cec is null then 0.0
            when hd.soil_cec between tsc.optimal_min and tsc.optimal_max then 1.0
            when hd.soil_cec >= tsc.acceptable_min and tsc.soil_cec < tsc.optimal_min then
                (hd.soil_cec - tsc.acceptable_min) / (tsc.optimal_min - tsc.acceptable_min) * 0.5 + 0.5
            when hd.soil_cec > tsc.optimal_max and hd.soil_cec <= tsc.acceptable_max then
                1.0 - ((hd.soil_cec - tsc.optimal_max) / (tsc.acceptable_max - tsc.optimal_max) * 0.5)
            else 0.0
        end as soil_cec_score,
        case
            when hd.clay is null then 0.0
            when hd.clay between tcl.optimal_min and tcl.optimal_max then 1.0
            when hd.clay >= tcl.acceptable_min and hd.clay < tcl.optimal_min then
                (hd.clay - tcl.acceptable_min) / (tcl.optimal_min - tcl.acceptable_min) * 0.5 + 0.5
            when hd.clay > tcl.optimal_max and hd.clay <= tcl.acceptable_max then
                1.0 - ((hd.clay - tcl.optimal_max) / (tcl.acceptable_max - tcl.optimal_max) * 0.5)
            else 0.0
        end as clay_score,
        case
            when hd.silt is null then 0.0
            when hd.silt between tsi.optimal_min and tsi.optimal_max then 1.0
            when hd.silt >= tsi.acceptable_min and tsi.silt < tsi.optimal_min then
                (hd.silt - tsi.acceptable_min) / (tsi.optimal_min - tsi.acceptable_min) * 0.5 + 0.5
            when hd.silt > tsi.optimal_max and hd.silt <= tsi.acceptable_max then
                1.0 - ((hd.silt - tsi.optimal_max) / (tsi.acceptable_max - tsi.optimal_max) * 0.5)
            else 0.0
        end as silt_score,
        case
            when hd.sand is null then 0.0
            when hd.sand between tsa.optimal_min and tsa.optimal_max then 1.0
            when hd.sand >= tsa.acceptable_min and tsa.sand < tsa.optimal_min then
                (hd.sand - tsa.acceptable_min) / (tsa.optimal_min - tsa.acceptable_min) * 0.5 + 0.5
            when hd.sand > tsa.optimal_max and hd.sand <= tsa.acceptable_max then
                1.0 - ((hd.sand - tsa.optimal_max) / (tsa.acceptable_max - tsa.optimal_max) * 0.5)
            else 0.0
        end as sand_score
    from harmonized_data hd
    left join dim_thresholds tn on hd.agricultural_period = tn.period and tn.feature = 'ndvi'
    left join dim_thresholds tt on hd.agricultural_period = tt.period and tt.feature = 'temperature'
    left join dim_thresholds tr on hd.agricultural_period = tr.period and tr.feature = 'rainfall'
    left join dim_thresholds tsp on tsp.period = 'static' and tsp.feature = 'soil_ph'
    left join dim_thresholds tsc on tsc.period = 'static' and tsc.feature = 'soil_cec'
    left join dim_thresholds tcl on tcl.period = 'static' and tcl.feature = 'clay'
    left join dim_thresholds tsi on tsi.period = 'static' and tsi.feature = 'silt'
    left join dim_thresholds tsa on tsa.period = 'static' and tsa.feature = 'sand'
)
select
    field_id,
    date,
    agricultural_period,
    ndvi,
    temperature,
    rainfall,
    soil_ph,
    soil_cec,
    clay,
    silt,
    sand,
    ndvi_score,
    temperature_score,
    rainfall_score,
    soil_ph_score,
    soil_cec_score,
    clay_score,
    silt_score,
    sand_score,
    (temperature_score * 0.4) +
    (rainfall_score * 0.3) +
    (ndvi_score * 0.2) +
    ((soil_ph_score + soil_cec_score + clay_score + silt_score + sand_score) / 5 * 0.1) as total_suitability_score
from calculate_scores
