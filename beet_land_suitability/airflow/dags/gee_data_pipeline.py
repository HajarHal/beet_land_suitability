from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
import os
import json
import pandas as pd
import ee
import geopandas as gpd
from shapely.geometry import mapping
from airflow.models import Variable
from airflow.dags_dependencies.utils import (
    initialize_gee,
    load_cropland_polygons,
    reproject_and_resample,
    clip_to_polygons,
    zonal_statistics,
    extract_data_to_dataframe,
    get_image_collection_by_date,
    get_static_image
)
PROJECT_ID = Variable.get("gcp_project_id")
BIGQUERY_DATASET = Variable.get("bigquery_dataset_id", default_var="gee_raw_data")
GEE_START_DATE = "2020-01-01"
GEE_END_DATE = "2024-12-31"
GEE_SCALE = 30 # meters
GEOJSON_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "dags_dependencies", "morocco_cropland.geojson")
def _load_geojson_to_bq(geojson_path, project_id, dataset_id, table_id, **kwargs):
    initialize_gee()
    gdf = load_cropland_polygons(geojson_path)
    records = []
    for index, row in gdf.iterrows():
        record = {
            "field_id": row["field_id"],
            "geometry": json.dumps(mapping(row["geometry"])) # Store geometry as GeoJSON string
        }
        records.append(record)
    df = pd.DataFrame(records)
    df.to_gbq(
        destination_table=f"{dataset_id}.{table_id}",
        project_id=project_id,
        if_exists="replace",
        progress_bar=False
    )
    print(f"Successfully loaded {len(df)} cropland polygons to BigQuery table {dataset_id}.{table_id}")
def _extract_ndvi_data(geojson_path, project_id, dataset_id, table_id, start_date, end_date, scale, **kwargs):
    initialize_gee()
    gdf = load_cropland_polygons(geojson_path)
    feature_collection = ee.FeatureCollection(json.loads(gdf.to_json()))
    ndvi_collection = get_image_collection_by_date("MODIS/051/MOD13Q1", start_date, end_date)
    all_ndvi_data = []
    for date_str in pd.date_range(start=start_date, end=end_date, freq="MS").strftime("%Y-%m-%d").tolist():
        img = ndvi_collection.filterDate(date_str, pd.to_datetime(date_str) + pd.DateOffset(months=1)).mean()
        if img.bandNames().size().getInfo() == 0: # Check if image is empty
            print(f"No NDVI image for {date_str}, skipping.")
            continue
        ndvi_img = img.select("NDVI").rename("ndvi")
        harmonized_img = reproject_and_resample(ndvi_img, target_scale=scale)
        clipped_img = clip_to_polygons(harmonized_img, feature_collection)
        zonal_stats_fc = zonal_statistics(clipped_img, feature_collection, ee.Reducer.mean(), scale)
        df = extract_data_to_dataframe(zonal_stats_fc, "ndvi", date=date_str)
        all_ndvi_data.append(df)
    if all_ndvi_data:
        final_df = pd.concat(all_ndvi_data, ignore_index=True)
        final_df.to_gbq(
            destination_table=f"{dataset_id}.{table_id}",
            project_id=project_id,
            if_exists="append", # Append data for different dates
            progress_bar=False
        )
        print(f"Successfully loaded {len(final_df)} NDVI records to BigQuery table {dataset_id}.{table_id}")
    else:
        print("No NDVI data extracted.")
def _extract_temperature_data(geojson_path, project_id, dataset_id, table_id, start_date, end_date, scale, **kwargs):
    initialize_gee()
    gdf = load_cropland_polygons(geojson_path)
    feature_collection = ee.FeatureCollection(json.loads(gdf.to_json()))
    temp_collection = get_image_collection_by_date("ECMWF/ERA5_LAND/DAILY_AGGR", start_date, end_date)
    all_temp_data = []
    for date_str in pd.date_range(start=start_date, end=end_date, freq="MS").strftime("%Y-%m-%d").tolist():
        img = temp_collection.filterDate(date_str, pd.to_datetime(date_str) + pd.DateOffset(months=1)).mean()
        if img.bandNames().size().getInfo() == 0:
            print(f"No Temperature image for {date_str}, skipping.")
            continue
        temp_img = img.select("temperature_2m").subtract(273.15).rename("temperature")
        harmonized_img = reproject_and_resample(temp_img, target_scale=scale)
        clipped_img = clip_to_polygons(harmonized_img, feature_collection)
        zonal_stats_fc = zonal_statistics(clipped_img, feature_collection, ee.Reducer.mean(), scale)
        df = extract_data_to_dataframe(zonal_stats_fc, "temperature", date=date_str)
        all_temp_data.append(df)
    if all_temp_data:
        final_df = pd.concat(all_temp_data, ignore_index=True)
        final_df.to_gbq(
            destination_table=f"{dataset_id}.{table_id}",
            project_id=project_id,
            if_exists="append",
            progress_bar=False
        )
        print(f"Successfully loaded {len(final_df)} Temperature records to BigQuery table {dataset_id}.{table_id}")
    else:
        print("No Temperature data extracted.")
def _extract_rainfall_data(geojson_path, project_id, dataset_id, table_id, start_date, end_date, scale, **kwargs):
    initialize_gee()
    gdf = load_cropland_polygons(geojson_path)
    feature_collection = ee.FeatureCollection(json.loads(gdf.to_json()))
    rainfall_collection = get_image_collection_by_date("UCSB-CHG/CHIRPS/DAILY", start_date, end_date)
    all_rainfall_data = []
    for date_str in pd.date_range(start=start_date, end=end_date, freq="MS").strftime("%Y-%m-%d").tolist():
        img = rainfall_collection.filterDate(date_str, pd.to_datetime(date_str) + pd.DateOffset(months=1)).sum()
        if img.bandNames().size().getInfo() == 0:
            print(f"No Rainfall image for {date_str}, skipping.")
            continue
        rainfall_img = img.select("precipitation").rename("rainfall")
        harmonized_img = reproject_and_resample(rainfall_img, target_scale=scale)
        clipped_img = clip_to_polygons(harmonized_img, feature_collection)
        zonal_stats_fc = zonal_statistics(clipped_img, feature_collection, ee.Reducer.sum(), scale)
        df = extract_data_to_dataframe(zonal_stats_fc, "rainfall", date=date_str)
        all_rainfall_data.append(df)
    if all_rainfall_data:
        final_df = pd.concat(all_rainfall_data, ignore_index=True)
        final_df.to_gbq(
            destination_table=f"{dataset_id}.{table_id}",
            project_id=project_id,
            if_exists="append",
            progress_bar=False
        )
        print(f"Successfully loaded {len(final_df)} Rainfall records to BigQuery table {dataset_id}.{table_id}")
    else:
        print("No Rainfall data extracted.")
def _extract_soil_data(geojson_path, project_id, dataset_id, table_id, scale, **kwargs):
    initialize_gee()
    gdf = load_cropland_polygons(geojson_path)
    feature_collection = ee.FeatureCollection(json.loads(gdf.to_json()))
    soil_ph = ee.Image("ISRIC/soilgrids/v200/properties/phh2o/mean/0-5cm").select("phh2o").rename("soil_ph")
    soil_cec = ee.Image("ISRIC/soilgrids/v200/properties/cec/mean/0-5cm").select("cec").rename("soil_cec")
    soil_clay = ee.Image("ISRIC/soilgrids/v200/properties/clay/mean/0-5cm").select("clay").rename("clay")
    soil_silt = ee.Image("ISRIC/soilgrids/v200/properties/silt/mean/0-5cm").select("silt").rename("silt")
    soil_sand = ee.Image("ISRIC/soilgrids/v200/properties/sand/mean/0-5cm").select("sand").rename("sand")
    soil_image_combined = ee.Image.cat([soil_ph, soil_cec, soil_clay, soil_silt, soil_sand])
    harmonized_img = reproject_and_resample(soil_image_combined, target_scale=scale)
    clipped_img = clip_to_polygons(harmonized_img, feature_collection)
    zonal_stats_fc = zonal_statistics(clipped_img, feature_collection, ee.Reducer.mean(), scale)
    data = []
    for feature in zonal_stats_fc.getInfo()["features"]:
        properties = feature["properties"]
        field_id = properties.get("field_id")
        if field_id:
            row = {
                "field_id": field_id,
                "soil_ph": properties.get("soil_ph"),
                "soil_cec": properties.get("soil_cec"),
                "clay": properties.get("clay"),
                "silt": properties.get("silt"),
                "sand": properties.get("sand"),
                "lat": feature["geometry"]["coordinates"][1] if feature["geometry"] else None, # Centroid lat
                "lon": feature["geometry"]["coordinates"][0] if feature["geometry"] else None  # Centroid lon
            }
            data.append(row)
    df = pd.DataFrame(data)
    df.to_gbq(
        destination_table=f"{dataset_id}.{table_id}",
        project_id=project_id,
        if_exists="replace",
        progress_bar=False
    )
    print(f"Successfully loaded {len(df)} Soil records to BigQuery table {dataset_id}.{table_id}")
with DAG(
    dag_id="gee_data_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["gee", "bigquery", "data_engineering"],
    params={
        "geojson_file_path": GEOJSON_FILE_PATH,
        "gcp_project_id": PROJECT_ID,
        "bigquery_dataset_id": BIGQUERY_DATASET,
        "gee_start_date": GEE_START_DATE,
        "gee_end_date": GEE_END_DATE,
        "gee_scale": GEE_SCALE,
    }
) as dag:
    create_dataset = BigQueryCreateEmptyTableOperator(
        task_id="create_bigquery_dataset",
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        table_id="dummy_table_for_dataset_creation", # A dummy table to ensure dataset exists
        gcp_conn_id="google_cloud_default",
        dag=dag,
    )
    load_cropland_polygons_task = PythonOperator(
        task_id="load_cropland_polygons_to_bigquery",
        python_callable=_load_geojson_to_bq,
        op_kwargs={
            "geojson_path": "{{ params.geojson_file_path }}",
            "project_id": "{{ params.gcp_project_id }}",
            "dataset_id": "{{ params.bigquery_dataset_id }}",
            "table_id": "cropland_polygons",
        },
        dag=dag,
    )
    extract_ndvi_task = PythonOperator(
        task_id="extract_ndvi_data",
        python_callable=_extract_ndvi_data,
        op_kwargs={
            "geojson_path": "{{ params.geojson_file_path }}",
            "project_id": "{{ params.gcp_project_id }}",
            "dataset_id": "{{ params.bigquery_dataset_id }}",
            "table_id": "ndvi_raw",
            "start_date": "{{ params.gee_start_date }}",
            "end_date": "{{ params.gee_end_date }}",
            "scale": "{{ params.gee_scale }}",
        },
        dag=dag,
    )
    extract_temperature_task = PythonOperator(
        task_id="extract_temperature_data",
        python_callable=_extract_temperature_data,
        op_kwargs={
            "geojson_path": "{{ params.geojson_file_path }}",
            "project_id": "{{ params.gcp_project_id }}",
            "dataset_id": "{{ params.bigquery_dataset_id }}",
            "table_id": "temperature_raw",
            "start_date": "{{ params.gee_start_date }}",
            "end_date": "{{ params.gee_end_date }}",
            "scale": "{{ params.gee_scale }}",
        },
        dag=dag,
    )
    extract_rainfall_task = PythonOperator(
        task_id="extract_rainfall_data",
        python_callable=_extract_rainfall_data,
        op_kwargs={
            "geojson_path": "{{ params.geojson_file_path }}",
            "project_id": "{{ params.gcp_project_id }}",
            "dataset_id": "{{ params.bigquery_dataset_id }}",
            "table_id": "rainfall_raw",
            "start_date": "{{ params.gee_start_date }}",
            "end_date": "{{ params.gee_end_date }}",
            "scale": "{{ params.gee_scale }}",
        },
        dag=dag,
    )
    extract_soil_task = PythonOperator(
        task_id="extract_soil_data",
        python_callable=_extract_soil_data,
        op_kwargs={
            "geojson_path": "{{ params.geojson_file_path }}",
            "project_id": "{{ params.gcp_project_id }}",
            "dataset_id": "{{ params.bigquery_dataset_id }}",
            "table_id": "soil_raw",
            "scale": "{{ params.gee_scale }}",
        },
        dag=dag,
    )
    trigger_dbt_seed = BashOperator(
        task_id="trigger_dbt_seed",
        bash_command=f"cd {os.path.join(os.environ["AIRFLOW_HOME"], "dbt")} && dbt seed --profile beet_land_suitability --target dev",
        env={
            "BIGQUERY_PROJECT": PROJECT_ID,
            "BIGQUERY_DATASET": BIGQUERY_DATASET,
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""), # Ensure this is set in Airflow env
        },
        dag=dag,
    )
    trigger_dbt_run = BashOperator(
        task_id="trigger_dbt_run",
        bash_command=f"cd {os.path.join(os.environ["AIRFLOW_HOME"], "dbt")} && dbt run --profile beet_land_suitability --target dev",
        env={
            "BIGQUERY_PROJECT": PROJECT_ID,
            "BIGQUERY_DATASET": BIGQUERY_DATASET,
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
        dag=dag,
    )
    trigger_dbt_test = BashOperator(
        task_id="trigger_dbt_test",
        bash_command=f"cd {os.path.join(os.environ["AIRFLOW_HOME"], "dbt")} && dbt test --profile beet_land_suitability --target dev",
        env={
            "BIGQUERY_PROJECT": PROJECT_ID,
            "BIGQUERY_DATASET": BIGQUERY_DATASET,
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
        dag=dag,
    )
    create_dataset >> load_cropland_polygons_task
    create_dataset >> [extract_ndvi_task, extract_temperature_task, extract_rainfall_task, extract_soil_task]
    all_extraction_tasks = [
        load_cropland_polygons_task,
        extract_ndvi_task,
        extract_temperature_task,
        extract_rainfall_task,
        extract_soil_task
    ]
    all_extraction_tasks >> trigger_dbt_seed >> trigger_dbt_run >> trigger_dbt_test
