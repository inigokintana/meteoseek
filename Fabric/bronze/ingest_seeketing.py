# CELL 1
#
# Manually created keyvault fabric-meteoseek-kv in Azure with several secrets in it
# seeketing-login, seeketing-password and seeketing-app-id
# Checking we have access

from notebookutils import mssparkutils

vault_uri = "https://fabric-meteoseek-kv.vault.azure.net/"

login = mssparkutils.credentials.getSecret(
    vault_uri,
    "seeketing-login"
)

password = mssparkutils.credentials.getSecret(
    vault_uri,
    "seeketing-password"
)


app_id = mssparkutils.credentials.getSecret(
    vault_uri,
    "seeketing-app-id"
)

# This changes every week not need to save it in secre
# api_key = mssparkutils.credentials.getSecret(
#     vault_uri,
#     "seeketing-api-key"
# )

print(login)
print(password)
print(app_id)
#print(api_key)
print("Secrets retrieved successfully")

# CELL 2
#
# Manually created keyvault fabric-meteoseek-kv in Azure with several secrets in it
# seeketing-login, seeketing-password and seeketing-app-id
# Checking we have access

from notebookutils import mssparkutils

vault_uri = "https://fabric-meteoseek-kv.vault.azure.net/"

login = mssparkutils.credentials.getSecret(
    vault_uri,
    "seeketing-login"
)

password = mssparkutils.credentials.getSecret(
    vault_uri,
    "seeketing-password"
)


app_id = mssparkutils.credentials.getSecret(
    vault_uri,
    "seeketing-app-id"
)

# This changes every week not need to save it in secre
# api_key = mssparkutils.credentials.getSecret(
#     vault_uri,
#     "seeketing-api-key"
# )

print(login)
print(password)
print(app_id)
#print(api_key)
print("Secrets retrieved successfully")



# CELL 3
# Welcome to your new notebook
# Type here in the cell editor to add code!
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Seeketing ingestion
# MAGIC
# MAGIC Ingest raw Seeketing Observer data into the Bronze Delta layer.
# MAGIC
# MAGIC Sources (CMDB.md §2.2):
# MAGIC - EP-SEK-02 `getAppZonesPublic`    -> Moving between zones
# MAGIC - EP-SEK-03 `getZoneVisitsH`       -> TBL-03 `seeketing_zone_visits_h`
# MAGIC - EP-SEK-05 `getSensorsData`       -> TBL-04 `seeketing_sensors_data` ** This data is not interesting
# MAGIC
# MAGIC Auth: `authRest` returns a session `key` (SEC-03) held in memory only for
# MAGIC the lifetime of this run; never persisted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Notebook parameters (widgets)

# COMMAND ----------
# dbutils is for Databricks not for Fabric
# dbutils.widgets.text("lakehouse", "demo", "Lakehouse name")
# dbutils.widgets.text("app_id", "", "Seeketing app_id (CMDB MAP-02 / SRC-SEK-05)")
# dbutils.widgets.text("start_date", "2026-08-01", "Start date (yyyy-MM-dd); empty = last 24h")
# dbutils.widgets.text("end_date", "", "End date (yyyy-MM-dd); empty = now")
#Thre is no dbutils in Fabric, use this intead::
# Notebook parameter (notebookutils)
# Python variables
# Spark config
# Fabric pipelines parameters

# Global setting
lakehouse = "Demo"
my_app_id = app_id
start_date = "2026-08-01"
end_date = "2026-08-31"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports + shared helpers

# COMMAND ----------

import datetime as dt
import json
import sys

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

start_ts = int(
    dt.datetime.strptime(start_date, "%Y-%m-%d")
    .replace(tzinfo=dt.timezone.utc)
    .timestamp()
)

end_ts = int(
    dt.datetime.strptime(end_date, "%Y-%m-%d")
    .replace(tzinfo=dt.timezone.utc)
    .timestamp()
)

spark = SparkSession.builder.getOrCreate()

# importing common library helper functions
sys.path.insert(0, "/lakehouse/default/Files/meteoseek/common")
from fabric_utils import (
    get_secret,
    seeketing_auth,
    seeketing_call,
    lakehouse_paths,
    write_bronze,
)

# dbutils only in databricks
# paths = lakehouse_paths(dbutils.widgets.get("lakehouse"))
paths = lakehouse_paths(lakehouse)
# app_id = dbutils.widgets.get("app_id").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Auth — get session key (SEC-03, in-memory only)

# COMMAND ----------

#app_id = get_secret("seeketing-app-id")
#api_key = get_secret("seeketing-api-key")
#if not login or not password:
#    raise RuntimeError("Missing Seeketing credentials. See CMDB.md SEC-02.")

session_key = seeketing_auth(login, password)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pull zone list (EP-SEK-02) — Bronze snapshot of dim_zone

# COMMAND ----------

zones = seeketing_call(session_key, "getAppZonesPublic", {"app_id": my_app_id})
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

# "zone_id":36427 Vitoria-Gasteiz global
# "type":"hh": By hours."dd": By days."ww": By weeks."mm": By months.
visits = seeketing_call(
    session_key,
    "getZoneVisitsH",
    {"app_id": my_app_id, 
     "last": start_ts,
     "end": end_ts,
     "zone_id":36427,
     "type":'dd'},
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

# sensors = seeketing_call(
#     session_key,
#     "getSensorsData",
#     {"app_id": app_id, 
#      "date_start": start_ts,
#      "date_end": end_ts},
# )
# if isinstance(sensors, dict):
#     sensors = sensors.get("data", sensors)
# sensors = sensors if isinstance(sensors, list) else [sensors]

# sensors_df = spark.createDataFrame(
#     [Row(payload=json.dumps(s, ensure_ascii=False, default=str), source_ep="EP-SEK-05")
#      for s in sensors]
# ).withColumn("payload_hash", F.sha2(F.col("payload"), 256))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Bronze (append-only, idempotent via payload hash)

# COMMAND ----------
print(paths)
print(paths["bronze"])

if zones_df.count() > 0:
    write_bronze(
        spark,
        zones_df,
        "seeketing_zones",
        dedupe_cols=["payload_hash"],
    )
    print(f"Wrote {zones_df.count()} zones.")

if visits_df.count() > 0:
    write_bronze(
        spark,
        visits_df,
        "seeketing_zone_visits_h",
        dedupe_cols=["payload_hash"],
    )
    print(f"Wrote {visits_df.count()} zone-visit rows.")

# if sensors_df.count() > 0:
#     write_bronze(
#         spark, sensors_df,
#         f"{paths['bronze']}/seeketing_sensors_data",
#         dedupe_cols=["payload_hash"],
#     )
#     print(f"Wrote {sensors_df.count()} sensor rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC Bronze is append-only. Silver types and keys the hourly footfall facts.


#CELL 4
df = spark.sql("SELECT * FROM Demo.dbo.seeketing_zones LIMIT 1000")
display(df)

# CELL 5
df = spark.sql("SELECT * FROM Demo.dbo.seeketing_zone_visits_h LIMIT 1000")
display(df)
