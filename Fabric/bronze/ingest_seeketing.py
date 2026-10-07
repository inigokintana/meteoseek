# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Seeketing ingestion
# MAGIC
# MAGIC Ingest raw Seeketing Observer data into the Bronze Delta layer.
# MAGIC
# MAGIC Sources (CMDB.md §2.2):
# MAGIC - EP-SEK-02 `getAppZonesPublic`    -> dim_zone (Bronze snapshot)
# MAGIC - EP-SEK-03 `getZoneVisitsH`       -> TBL-03 `seeketing_zone_visits_h`
# MAGIC - EP-SEK-05 `getSensorsData`       -> TBL-04 `seeketing_sensors_data`
# MAGIC
# MAGIC Auth: `authRest` returns a session `key` (SEC-03) held in memory only for
# MAGIC the lifetime of this run; never persisted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Notebook parameters (widgets)

# COMMAND ----------

dbutils.widgets.text("lakehouse", "meteoseek_lh", "Lakehouse name")
dbutils.widgets.text("app_id", "", "Seeketing app_id (CMDB MAP-02 / SRC-SEK-05)")
dbutils.widgets.text("start_date", "", "Start date (yyyy-MM-dd); empty = last 24h")
dbutils.widgets.text("end_date", "", "End date (yyyy-MM-dd); empty = now")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports + shared helpers

# COMMAND ----------

import datetime as dt
import json
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

sys.path.insert(0, "/lakehouse/default/Files/fabric/common")
from fabric_utils import (
    get_secret,
    seeketing_auth,
    seeketing_call,
    lakehouse_paths,
    write_bronze,
)

paths = lakehouse_paths(dbutils.widgets.get("lakehouse"))
app_id = dbutils.widgets.get("app_id").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Auth — get session key (SEC-03, in-memory only)

# COMMAND ----------

login = get_secret("seeketing-login")
password = get_secret("seeketing-password")
if not login or not password:
    raise RuntimeError("Missing Seeketing credentials. See CMDB.md SEC-02.")

session_key = seeketing_auth(login, password)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pull zone list (EP-SEK-02) — Bronze snapshot of dim_zone

# COMMAND ----------

zones = seeketing_call(session_key, "getAppZonesPublic", {"app_id": app_id})
if isinstance(zones, dict):
    zones = zones.get("data", zones)
zones = zones if isinstance(zones, list) else [zones]

zones_df = spark.createDataFrame(
    [Row(payload=json.dumps(z, ensure_ascii=False, default=str), source_ep="EP-SEK-02")
     for z in zones]
).withColumn("payload_hash", F.sha2(F.col("payload"), 256))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pull hourly zone visits (EP-SEK-03)

# COMMAND ----------

visits = seeketing_call(
    session_key,
    "getZoneVisitsH",
    {"app_id": app_id, "date_start": dbutils.widgets.get("start_date"),
     "date_end": dbutils.widgets.get("end_date")},
)
if isinstance(visits, dict):
    visits = visits.get("data", visits)
visits = visits if isinstance(visits, list) else [visits]

visits_df = spark.createDataFrame(
    [Row(payload=json.dumps(v, ensure_ascii=False, default=str), source_ep="EP-SEK-03")
     for v in visits]
).withColumn("payload_hash", F.sha2(F.col("payload"), 256))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pull sensor data (EP-SEK-05)

# COMMAND ----------

sensors = seeketing_call(
    session_key,
    "getSensorsData",
    {"app_id": app_id, "date_start": dbutils.widgets.get("start_date"),
     "date_end": dbutils.widgets.get("end_date")},
)
if isinstance(sensors, dict):
    sensors = sensors.get("data", sensors)
sensors = sensors if isinstance(sensors, list) else [sensors]

sensors_df = spark.createDataFrame(
    [Row(payload=json.dumps(s, ensure_ascii=False, default=str), source_ep="EP-SEK-05")
     for s in sensors]
).withColumn("payload_hash", F.sha2(F.col("payload"), 256))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Bronze (append-only, idempotent via payload hash)

# COMMAND ----------

if zones_df.count() > 0:
    write_bronze(
        spark, zones_df,
        f"{paths['bronze']}/seeketing_zones",
        dedupe_cols=["payload_hash"],
    )
    print(f"Wrote {zones_df.count()} zones.")

if visits_df.count() > 0:
    write_bronze(
        spark, visits_df,
        f"{paths['bronze']}/seeketing_zone_visits_h",
        dedupe_cols=["payload_hash"],
    )
    print(f"Wrote {visits_df.count()} zone-visit rows.")

if sensors_df.count() > 0:
    write_bronze(
        spark, sensors_df,
        f"{paths['bronze']}/seeketing_sensors_data",
        dedupe_cols=["payload_hash"],
    )
    print(f"Wrote {sensors_df.count()} sensor rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC Bronze is append-only. Silver types and keys the hourly footfall facts.
