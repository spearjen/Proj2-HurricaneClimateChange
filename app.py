import pandas
import pandas_gbq

# TODO: Set project_id to your Google Cloud Platform project ID.
project_id = "plucky-courier-292500"

# sql = """
# SELECT latitude, longitude, year, month, day, air_temperature, sea_surface_temp
# FROM `bigquery-public-data.noaa_icoads.icoads_core_1662_2000`
# WHERE year >= 1851
# AND latitude >=15.623
# AND latitude <=47.279
# AND longitude >=-98.965
# AND longitude <=-59.063
# """
sql = """
SELECT latitude, longitude, year, month, day, air_temperature, sea_surface_temp
FROM `bigquery-public-data.noaa_icoads.icoads_core_2001_2004`
WHERE latitude >=15.623
AND latitude <=47.279
AND longitude >=-98.965
AND longitude <=-59.063
"""
# TODO: Set table_id to the full destination table ID (including the
#       dataset ID).
table_id = 'hurricane_df.hurricane_df_2001-2004'

df = pandas.DataFrame(
#     {
#         "latitude": "latitude",
#         "longitude": "longitude",
#         "year": "year",
#         "month": "month",
#         "day": "day",
#         "air_temperature": "air_temperature",
#         "longitude": "sea_surface_temp",
#     }
# )

# years = 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017

# for year in years:

#     sql = """
#     SELECT latitude, longitude, year, month, day, air_temperature, sea_surface_temp
#     FROM `bigquery-public-data.noaa_icoads.icoads_core_{year}`
#     WHERE latitude >=15.623
#     AND latitude <=47.279
#     AND longitude >=-98.965
#     AND longitude <=-59.063
#     """

# df = pandas_gbq.read_gbq(sql, project_id=project_id)
pandas_gbq.to_gbq(df, table_id, project_id=project_id)