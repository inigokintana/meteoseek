# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Euskalmet ingestion
# MAGIC
# MAGIC Ingest raw Euskalmet data (station readings + region forecasts) into the
# Bronze Delta layer as append-only, audit-stamped JSON payloads.
# MAGIC
# MAGIC Sources (CMDB.md §2.1):
# MAGIC - EP-EUS-08 `stations_readings`  -> TBL-01 `euskalmet_station_readings`
# MAGIC - EP-EUS-05 `weather_region_zone` (fallback) + EP-EUS-04 location forecast
# MAGIC   -> TBL-02 `euskalmet_forecast_region`
# MAGIC
# MAGIC Auth: RS256 JWT signed with the private key held in the Fabric secret
# MAGIC `euskalmet-private-key` (SEC-01).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Notebook parameters (widgets)

# COMMAND ----------

dbutils.widgets.text("lakehouse", "meteoseek_lh", "Lakehouse name")
dbutils.widgets.text("euskalmet_base_url", "https://api.euskadi.eus", "Euskalmet API base URL")
dbutils.widgets.text("region_id", "", "Region ID (resolve via geolocations_regions)")
dbutils.widgets.text("zone_id", "", "Zone ID (resolve via geolocations_zone)")
dbutils.widgets.text("location_id", "", "Location ID (resolve via geolocations_locations)")
dbutils.widgets.text("start_hour", "", "Optional start datetime (yyyy-MM-dd HH:mm:ss); empty = last 24h")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports + shared helpers

# COMMAND ----------

import datetime as dt
import json
import sys
import urllib.request

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Import the shared helper module. In Fabric this file is placed under the
# Lakehouse `Files/fabric/` and added to the notebook via sys.path, or attached
# as a notebook resource. See Fabric/README.md.
sys.path.insert(0, "/lakehouse/default/Files/fabric/common")
from fabric_utils import (
    get_secret,
    euskalmet_headers,
    lakehouse_paths,
    write_bronze,
)

paths = lakehouse_paths(dbutils.widgets.get("lakehouse"))
base_url = dbutils.widgets.get("euskalmet_base_url").rstrip("/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Auth — build the RS256 JWT from the Key Vault private key

# COMMAND ----------

private_key_pem = get_secret("euskalmet-private-key")
if not private_key_pem:
    raise RuntimeError("Missing secret 'euskalmet-private-key'. See CMDB.md SEC-01.")

headers = euskalmet_headers(private_key_pem)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pull station readings (EP-EUS-08)

# COMMAND ----------

def euskalmet_get(path: str):
    """GET a Euskalmet endpoint and return the parsed JSON."""
    url = f"{base_url}/{path.lstrip('/')}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode("utf-8"))


readings_raw = euskalmet_get("stations_readings")
if isinstance(readings_raw, dict):
    readings_raw = readings_raw.get("data", readings_raw)
readings_rows = readings_raw if isinstance(readings_raw, list) else [readings_raw]

readings_df = spark.createDataFrame(
    [Row(payload=json.dumps(r, ensure_ascii=False, default=str), source_ep="EP-EUS-08")
     for r in readings_rows]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pull region-zone forecast (EP-EUS-05, fallback EP-EUS-04)

# COMMAND ----------

zone_id = dbutils.widgets.get("zone_id").strip()
location_id = dbutils.widgets.get("location_id").strip()
region_id = dbutils.widgets.get("region_id").strip()

# Prefer the location-level forecast (EP-EUS-04); fall back to zone/region level.
forecast_payloads = []
if location_id:
    forecast_payloads.append(
        (euskalmet_get(f"weather_region_zone_location/{location_id}"), "EP-EUS-04")
    )
elif zone_id:
    forecast_payloads.append(
        (euskalmet_get(f"weather_region_zone/{zone_id}"), "EP-EUS-05")
    )
elif region_id:
    forecast_payloads.append(
        (euskalmet_get(f"weather_region/{region_id}"), "EP-EUS-05")
    )

forecast_rows = []
for payload, ep in forecast_payloads:
    if isinstance(payload, dict):
        payload = payload.get("data", payload)
    items = payload if isinstance(payload, list) else [payload]
    forecast_rows.extend(
        Row(payload=json.dumps(i, ensure_ascii=False, default=str), source_ep=ep)
        for i in items
    )

forecast_df = spark.createDataFrame(forecast_rows) if forecast_rows else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write Bronze

# COMMAND ----------

# TBL-01 — station readings. Dedupe on the raw payload hash to stay idempotent
# while keeping Bronze append-only.
if readings_df.count() > 0:
    readings_df = readings_df.withColumn(
        "payload_hash", F.sha2(F.col("payload"), 256)
    )
    write_bronze(
        spark,
        readings_df,
        f"{paths['bronze']}/euskalmet_station_readings",
        dedupe_cols=["payload_hash"],
    )
    print(f"Wrote {readings_df.count()} station readings to Bronze.")

# TBL-02 — forecast. Dedupe on (source_ep, payload_hash).
if forecast_df is not None and forecast_df.count() > 0:
    forecast_df = forecast_df.withColumn("payload_hash", F.sha2(F.col("payload"), 256))
    write_bronze(
        spark,
        forecast_df,
        f"{paths['bronze']}/euskalmet_forecast_region",
        dedupe_cols=["source_ep", "payload_hash"],
    )
    print(f"Wrote {forecast_df.count()} forecast rows to Bronze.")
else:
    print("No forecast payload returned; region/zone/location IDs may be unset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC Bronze is append-only. The Silver notebook types and keys these payloads.
