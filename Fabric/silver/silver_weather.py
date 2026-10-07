# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Weather (Euskalmet)
# MAGIC
# MAGIC Parse raw Euskalmet Bronze JSON into typed, keyed Silver tables:
# MAGIC
# MAGIC - `fact_weather_hourly` (TBL-05)  key `(location_id, ts_hour)`
# MAGIC - `dim_location`       (TBL-08)  key `location_id`
# MAGIC
# MAGIC Grain: hour x location. Join key aligns with footfall on `ts_hour`.

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

import json
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType

spark = SparkSession.builder.getOrCreate()

sys.path.insert(0, "/lakehouse/default/Files/fabric/common")
from fabric_utils import lakehouse_paths, read_delta, upsert_delta, parse_ts

paths = lakehouse_paths(dbutils.widgets.get("lakehouse"))
location_id = dbutils.widgets.get("location_id").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Bronze station readings (TBL-01)

# COMMAND ----------

readings = read_delta(spark, f"{paths['bronze']}/euskalmet_station_readings")
if readings is None:
    raise RuntimeError("No Bronze station readings found. Run ingest_euskalmet first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse payloads into typed weather facts
# MAGIC
# MAGIC The exact field names inside the Euskalmet payload vary by endpoint and
# MAGIC station schema. We use `get_json_object` defensively and coalesce the
# MAGIC common meteorological variables (temperature, precipitation, wind, cloud).
# MAGIC Adjust the JSON paths below to the confirmed Euskalmet payload once the
# MAGIC first geolocation pull is done (CMDB.md §9 open item).

# COMMAND ----------

# Defensive extraction: try several plausible JSON paths for each measure.
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Silver `fact_weather_hourly` (TBL-05)

# COMMAND ----------

upsert_delta(
    spark,
    weather,
    f"{paths['silver']}/fact_weather_hourly",
    key_cols=["location_id", "ts_hour"],
)
print(f"Upserted {weather.count()} rows into fact_weather_hourly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC `fact_weather_hourly` is typed and keyed on `(location_id, ts_hour)`.
