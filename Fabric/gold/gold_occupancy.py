# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Occupancy, Forecast & Time dimension
# MAGIC
# MAGIC Build the BI-ready Gold layer:
# MAGIC
# MAGIC - `fact_occupancy_hourly` (TBL-09)  key `(location_id, zone_id, ts_hour)`
# MAGIC   = `fact_weather_hourly` JOIN `fact_footfall_hourly` on `ts_hour`
# MAGIC - `fact_forecast`          (TBL-10)  key `(location_id, ts_hour)`
# MAGIC - `dim_time`               (TBL-11)  key `ts_hour` (generated)
# MAGIC
# MAGIC The `(location_id / zone_id, ts_hour)` join is the single keyed surface
# MAGIC that both Power BI (Direct Lake) and the Hermes Agent read from.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("lakehouse", "meteoseek_lh", "Lakehouse name")
dbutils.widgets.text("location_id", "", "Location ID (Vitoria-Gasteiz)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

sys.path.insert(0, "/lakehouse/default/Files/fabric/common")
from fabric_utils import lakehouse_paths, read_delta, upsert_delta

paths = lakehouse_paths(dbutils.widgets.get("lakehouse"))
location_id = dbutils.widgets.get("location_id").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Silver fact tables

# COMMAND ----------

weather = read_delta(spark, f"{paths['silver']}/fact_weather_hourly")
footfall = read_delta(spark, f"{paths['silver']}/fact_footfall_hourly")

if weather is None or footfall is None:
    raise RuntimeError("Silver fact tables missing. Run silver notebooks first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build `fact_occupancy_hourly` (TBL-09)
# MAGIC
# MAGIC Cross join weather (per location) with footfall (per zone) on the common
# MAGIC hourly grain `ts_hour`. Every zone inherits the same city-level weather.

# COMMAND ----------

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

upsert_delta(
    spark, occupancy,
    f"{paths['gold']}/fact_occupancy_hourly",
    key_cols=["location_id", "zone_id", "ts_hour"],
)
print(f"Upserted {occupancy.count()} rows into fact_occupancy_hourly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build `fact_forecast` (TBL-10) from Silver weather
# MAGIC
# MAGIC The forecast is the weather projection (no footfall yet); the Hermes
# MAGIC agent joins it with the historical weather->footfall correlation at
# MAGIC query time (AGT-03).

# COMMAND ----------

forecast = weather.select(
    "location_id", "ts_hour",
    "temp_c", "precip_mm", "wind_kmh", "cloud_pct", "humidity_pct",
)

upsert_delta(
    spark, forecast,
    f"{paths['gold']}/fact_forecast",
    key_cols=["location_id", "ts_hour"],
)
print(f"Upserted {forecast.count()} rows into fact_forecast.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build `dim_time` (TBL-11) — generated hour dimension

# COMMAND ----------

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

upsert_delta(spark, dim_time, f"{paths['gold']}/dim_time", key_cols=["ts_hour"])
print(f"Upserted {dim_time.count()} rows into dim_time.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC Gold layer is BI-ready. Point the Power BI Direct Lake semantic model at
# MAGIC `fact_occupancy_hourly`, `fact_forecast`, `dim_zone`, `dim_location`,
# MAGIC and `dim_time`.
