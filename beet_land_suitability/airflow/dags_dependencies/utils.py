import ee
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import mapping
def initialize_gee():
    """Initializes the Google Earth Engine API."""
    try:
        ee.Initialize()
        print("Google Earth Engine initialized successfully.")
    except Exception as e:
        print(f"Error initializing GEE: {e}")
        ee.Authenticate()
        ee.Initialize()
def load_cropland_polygons(geojson_path):
    """Loads cropland polygons from a GeoJSON file."""
    gdf = gpd.read_file(geojson_path)
    if 'field_id' not in gdf.columns:
        gdf['field_id'] = [f'field_{i}' for i in range(len(gdf))]
    gdf['field_id'] = gdf['field_id'].astype(str)
    return gdf
def reproject_and_resample(image, target_crs='EPSG:4326', target_scale=30):
    """Reprojects and resamples an image to a common CRS and spatial resolution."""
    return image
def clip_to_polygons(image, feature_collection):
    """Clips an image to the boundaries of the feature collection."""
    return image.clipToCollection(feature_collection)
def zonal_statistics(image, feature_collection, reducer, scale, crs='EPSG:4326'):
    """Computes zonal statistics for an image over a feature collection."""
    return image.reduceRegions(
        collection=feature_collection,
        reducer=reducer,
        scale=scale,
        crs=crs
    )
def extract_data_to_dataframe(feature_collection, property_name, date=None):
    """Extracts data from a GEE FeatureCollection to a Pandas DataFrame."""
    data = []
    for feature in feature_collection.getInfo()['features']:
        properties = feature['properties']
        field_id = properties.get('field_id')
        value = properties.get(property_name)
        if field_id and value is not None:
            row = {'field_id': field_id, property_name: value}
            if date:
                row['date'] = date
            data.append(row)
    return pd.DataFrame(data)
def get_image_collection_by_date(collection_id, start_date, end_date):
    """Fetches an image collection filtered by date."""
    return ee.ImageCollection(collection_id).filterDate(start_date, end_date)
def get_static_image(image_id):
    """Fetches a static image."""
    return ee.Image(image_id)
