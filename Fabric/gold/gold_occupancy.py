# CELL 1
#
# Gold — Occupancy, Forecast & Time dimension
#
# Build the BI-ready Gold layer:
#
# - fact_occupancy_hourly  (TBL-09)  key (location_id, zone_id, ts_hour)
#   = fact_weather_hourly JOIN fact_footfall_hourly on ts_hour
# - fact_forecast          (TBL-10)  key (location_id, ts_hour)
# - dim_time               (TBL-11)  key ts_hour (generated)
#
# The (location_id / zone_id, ts_hour) join is the single keyed surface that
# both Power BI (Direct Lake) and the Hermes Agent read from.
#
# NOTE: no dbutils in Fabric — plain Python variables.

# Global settings
lakehouse = "Demo"
location_id = ""   # Location ID (Vitoria-Gasteiz)

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
# Load Silver fact tables

weather = read_table(spark, "fact_weather_hourly")
footfall = read_table(spark, "fact_footfall_hourly")

if weather is None or footfall is None:
    raise RuntimeError("Silver fact tables missing. Run silver notebooks first.")

# CELL 4
#
# Build fact_occupancy_hourly (TBL-09)
#
# Cross join weather (per location) with footfall (per zone) on the common
# hourly grain ts_hour. Every zone inherits the same city-level weather.

occupancy = (
    weather.alias("w")
    .join(footfall.alias("f"), on="ts_hour", how="inner")
    .select(
        F.col("w.location_id").alias("location_id"),
        F.col("f.zone_id").alias("zone_id"),
        "ts_hour",
        F.col("f.visits").alias("visits"),
        F.col("f.visitors_unique").alias("visitors_unique"),
        F.col("f.recurrents").alias("recurrents"),
        F.col("f.visittime_avg").alias("visittime_avg"),
        F.col("w.temp_c").alias("temp_c"),
        F.col("w.precip_mm").alias("precip_mm"),
        F.col("w.wind_kmh").alias("wind_kmh"),
        F.col("w.cloud_pct").alias("cloud_pct"),
        F.col("w.humidity_pct").alias("humidity_pct"),
    )
)

upsert_table(
    spark, occupancy,
    "fact_occupancy_hourly",
    key_cols=["location_id", "zone_id", "ts_hour"],
)
print(f"Upserted {occupancy.count()} rows into fact_occupancy_hourly.")

# CELL 5
#
# Build fact_forecast (TBL-10) from Silver weather
#
# The forecast is the weather projection (no footfall yet); the Hermes agent
# joins it with the historical weather->footfall correlation at query time
# (AGT-03).

forecast = weather.select(
    "location_id", "ts_hour",
    "temp_c", "precip_mm", "wind_kmh", "cloud_pct", "humidity_pct",
)

upsert_table(
    spark, forecast,
    "fact_forecast",
    key_cols=["location_id", "ts_hour"],
)
print(f"Upserted {forecast.count()} rows into fact_forecast.")

# CELL 6
#
# Build dim_time (TBL-11) — generated hour dimension

dim_time = (
    weather.select("ts_hour")
    .union(footfall.select("ts_hour"))
    .distinct()
    .withColumn("date", F.to_date("ts_hour"))
    .withColumn("hour", F.hour("ts_hour"))
    .withColumn("day_of_week", F.dayofweek("ts_hour"))
    .withColumn("is_weekend", F.when(F.dayofweek("ts_hour").isin(1, 7), 1).otherwise(0))
    .select("ts_hour", "date", "hour", "day_of_week", "is_weekend")
)

upsert_table(spark, dim_time, "dim_time", key_cols=["ts_hour"])
print(f"Upserted {dim_time.count()} rows into dim_time.")
