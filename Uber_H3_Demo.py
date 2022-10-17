# Databricks notebook source
# DBTITLE 1,Install Uber H3, folium, and geojson through pip
pip install h3

# COMMAND ----------

# DBTITLE 1,Imports
import h3 
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from shapely import geometry
from shapely.geometry import Polygon, MultiPolygon
import plotly.express as px

# COMMAND ----------

# DBTITLE 1,Load in dataset
# Specify where your data is
CSV_FILEPATH = "/dbfs/FileStore/shared_uploads/ThompsonAlex@JohnDeere.com/uber.csv"

# import csv of geospatial data
df = pd.read_csv(CSV_FILEPATH)

# Display data
display(df)

# COMMAND ----------

# DBTITLE 1,Apply a lambda function to get hex indexes
# function to take latitude and longtiude and produce hex index
# takes row, needed h3 index level, and the name of the latitude and longitude columns in the dataframe
def long_lat_to_h3 (row, res, lat_name, long_name):
  return h3.geo_to_h3(row[str(lat_name)], row[str(long_name)], int(res))

# Apply a lambda of the function to generate a column with the hex indexes
df['hex_L8'] = df.apply (lambda row: long_lat_to_h3(row, 8, 'pickup_latitude', 'pickup_longitude'), axis=1)

# Display data
display(df)

# COMMAND ----------

# DBTITLE 1,Doing hex aggregation in Pandas
# Do a group by on hex index and aggregate fare amounts so we can see what far amounts look like in each hex
df_aggregation = df.groupby(by='hex_L8').agg({'fare_amount': ['mean', 'min', 'max', 'count']})
# Fix column names
df_aggregation.columns = ['fare_amount_mean', 'fare_amount_min', 'fare_amount_max', 'count']
# Add back in hex indexes
df_aggregation = df_aggregation.reset_index()
# Remove hexes with a low number of records
df_aggregation = df_aggregation[df_aggregation['count'] >= 30]
# Display the data
display(df_aggregation)

# COMMAND ----------

# DBTITLE 1,Define Function to Visualizes Hexes
def plot_hexes(df, hex_column, col_to_color, title=None):
  hex_list = df[str(hex_column)] # Get a list of hex ids from dataframe
  polygon_list = [[hex_index, Polygon(h3.h3_to_geo_boundary(hex_index, geo_json=True))] for hex_index in hex_list] # using h3-py get turn the hex indexes into geojson, then make a list of the polygons
  # define a dictionary with hex ids and their corresponding geojsons
  geo_dict = {} 
  geo_dict["type"] = "FeatureCollection" 
  geo_dict["features"] = [{"type": "Feature", 'properties':{str(hex_column): a[0]}, "geometry": a[1]} for a in [[b[0], geometry.mapping(b[1])] for b in polygon_list]]
  for i,x in enumerate(geo_dict['features']):
    x['id'] = i
  # Set center of visualization
  center = polygon_list[0][1].centroid.wkt.replace('(','').replace(')','').split(' ')[1:]
  # define a choropleth map with the dataframe, dictionary that holds geojsons, the value column to visualize, and other aesthetic details
  fig = px.choropleth_mapbox(df, geojson=geo_dict, locations=str(hex_column), color=col_to_color,
                             featureidkey="properties." + str(hex_column),
                             mapbox_style="carto-positron",
                             zoom=15, center={"lat": float(center[1]), "lon": float(center[0])},
                             opacity=0.8,
                             title=title)
  # show figure
  fig.show()

# COMMAND ----------

# DBTITLE 1,Visualize hex aggregations
plot_hexes(df_aggregation, 'hex_L8', 'fare_amount_mean', title='Average Uber fare amounts by L8 Hex') # Call plotting function, and enjoy

# COMMAND ----------


