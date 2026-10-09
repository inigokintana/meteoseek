# CELL 1
#
# Silver — Weather (Euskalmet)
#
# Parse raw Euskalmet Bronze JSON into typed, keyed Silver tables:
#
# - fact_weather_hourly  (TBL-05)  key (location_id, ts_hour)
# - dim_location         (TBL-08)  key location_id
#
# Grain: hour x location. Join key aligns with footfall on ts_hour.
#
# NOTE: no dbutils in Fabric — plain Python variables.

# Global settings
lakehouse = "Demo"
location_id = ""   # Location ID (Vitoria-Gasteiz); resolve via geolocations_locations

# CELL 2
#
# Imports

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

sys.path.insert(0, "/lakehouse/default/Files/meteoseek/common")
from fabric_utils import read_table, upsert_table

# CELL 3
#
# Load Bronze station readings (TBL-01)

readings = read_table(spark, "euskalmet_station_readings")
if readings is None:
    raise RuntimeError("No Bronze station readings found. Run ingest_euskalmet first.")

# CELL 4
#
# Parse payloads into typed weather facts
#
# The exact field names inside the Euskalmet payload vary by endpoint and
# station schema. We use get_json_object defensively and coalesce the common
# meteorological variables (temperature, precipitation, wind, cloud). Adjust
# the JSON paths once the first geolocation pull confirms the payload shape
# (CMDB.md §9 open item).


def _json_any(col_expr, *paths):
    return F.coalesce(*[F.get_json_object(col_expr, p) for p in paths])


weather = (
    readings
    .withColumn("ts_hour", F.date_trunc("hour", F.to_timestamp(
        F.get_json_object("payload", "$.timestamp"), "yyyy-MM-dd HH:mm:ss")))
    .withColumn("location_id", F.lit(location_id).cast("long"))
    .withColumn(
        "temp_c",
        _json_any("payload", "$.temperature", "$.air_temperature", "$.temp", "$.ta")
        .cast("double"),
    )
    .withColumn(
        "precip_mm",
        _json_any("payload", "$.precipitation", "$.precip", "$.rain", "$.pr").cast("double"),
    )
    .withColumn(
        "wind_kmh",
        _json_any("payload", "$.wind_speed", "$.wind", "$.ws").cast("double"),
    )
    .withColumn(
        "cloud_pct",
        _json_any("payload", "$.cloud_cover", "$.clouds", "$.cloud").cast("double"),
    )
    .withColumn(
        "humidity_pct",
        _json_any("payload", "$.relative_humidity", "$.humidity", "$.rh").cast("double"),
    )
    .select(
        "location_id", "ts_hour",
        "temp_c", "precip_mm", "wind_kmh", "cloud_pct", "humidity_pct",
    )
    .dropna(subset=["ts_hour"])
)

# CELL 5
#
# Write Silver fact_weather_hourly (TBL-05)

upsert_table(
    spark,
    weather,
    "fact_weather_hourly",
    key_cols=["location_id", "ts_hour"],
)
print(f"Upserted {weather.count()} rows into fact_weather_hourly.")
