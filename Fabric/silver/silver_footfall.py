# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Footfall (Seeketing)
# MAGIC
# MAGIC Parse raw Seeketing Bronze JSON into typed, keyed Silver tables:
# MAGIC
# MAGIC - `fact_footfall_hourly` (TBL-06)  key `(zone_id, ts_hour)`
# MAGIC - `dim_zone`             (TBL-07)  key `zone_id`
# MAGIC
# MAGIC Grain: hour x zone. The `timestamp` field from `getZoneVisitsH` is
# MAGIC truncated to the hour to align with `fact_weather_hourly`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

dbutils.widgets.text("lakehouse", "meteoseek_lh", "Lakehouse name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports

# COMMAND ----------

import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

sys.path.insert(0, "/lakehouse/default/Files/fabric/common")
from fabric_utils import lakehouse_paths, read_delta, upsert_delta

paths = lakehouse_paths(dbutils.widgets.get("lakehouse"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Bronze visits (TBL-03) + zones (dim_zone snapshot)

# COMMAND ----------

visits = read_delta(spark, f"{paths['bronze']}/seeketing_zone_visits_h")
zones = read_delta(spark, f"{paths['bronze']}/seeketing_zones")

if visits is None:
    raise RuntimeError("No Bronze zone visits found. Run ingest_seeketing first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build `dim_zone` (TBL-07) from the latest zone snapshot
# MAGIC
# MAGIC `getAppZonesPublic` returns zone metadata (id, name, nodes, status).
# MAGIC We keep the most recent payload per zone id.

# COMMAND ----------

if zones is not None:
    dim_zone = (
        zones
        .withColumn("zone_id", F.get_json_object("payload", "$.id").cast("long"))
        .withColumn("zone_name", F.get_json_object("payload", "$.name"))
        .withColumn("short_name", F.get_json_object("payload", "$.short_name"))
        .withColumn("active", F.get_json_object("payload", "$.active").cast("int"))
        .withColumn("status", F.get_json_object("payload", "$.status").cast("int"))
        .withColumn("nodes", F.get_json_object("payload", "$.nodes"))
        .dropna(subset=["zone_id"])
        .dropDuplicates(["zone_id"])
        .select("zone_id", "zone_name", "short_name", "active", "status", "nodes")
    )
    upsert_delta(spark, dim_zone, f"{paths['silver']}/dim_zone", key_cols=["zone_id"])
    print(f"Upserted {dim_zone.count()} zones into dim_zone.")
else:
    print("No zone snapshot in Bronze; skipping dim_zone.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build `fact_footfall_hourly` (TBL-06)
# MAGIC
# MAGIC Fields from `getZoneVisitsH`: visits, new_visits, recurrents,
# MAGIC visitors_unique, visittime_avg, visit_time_zone_avg, presencetime_avg.

# COMMAND ----------

footfall = (
    visits
    .withColumn("zone_id", F.get_json_object("payload", "$.zone_id").cast("long"))
    .withColumn(
        "ts_hour",
        F.date_trunc("hour", F.to_timestamp(
            F.get_json_object("payload", "$.timestamp"), "yyyy-MM-dd HH:mm:ss")),
    )
    .withColumn("visits", F.get_json_object("payload", "$.visits").cast("long"))
    .withColumn("new_visits", F.get_json_object("payload", "$.new_visits").cast("long"))
    .withColumn("recurrents", F.get_json_object("payload", "$.recurrents").cast("long"))
    .withColumn("visitors_unique", F.get_json_object("payload", "$.visitors_unique").cast("long"))
    .withColumn("visittime_avg", F.get_json_object("payload", "$.visittime_avg").cast("double"))
    .withColumn("visit_time_zone_avg", F.get_json_object("payload", "$.visit_time_zone_avg").cast("double"))
    .withColumn("presencetime_avg", F.get_json_object("payload", "$.presencetime_avg").cast("double"))
    .dropna(subset=["zone_id", "ts_hour"])
    .select(
        "zone_id", "ts_hour",
        "visits", "new_visits", "recurrents", "visitors_unique",
        "visittime_avg", "visit_time_zone_avg", "presencetime_avg",
    )
)

upsert_delta(
    spark, footfall,
    f"{paths['silver']}/fact_footfall_hourly",
    key_cols=["zone_id", "ts_hour"],
)
print(f"Upserted {footfall.count()} rows into fact_footfall_hourly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC `fact_footfall_hourly` + `dim_zone` are typed and keyed on `(zone_id, ts_hour)`.
