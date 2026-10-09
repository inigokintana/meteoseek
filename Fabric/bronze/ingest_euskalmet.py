# CELL 1
#
# Manually created keyvault fabric-meteoseek-kv in Azure with Euskalmet secrets:
# euskalmet-fingerprint, euskalmet-priv-key, euskalmet-pub-key
# Checking we have access

from notebookutils import mssparkutils

vault_uri = "https://fabric-meteoseek-kv.vault.azure.net/"

fingerprint = mssparkutils.credentials.getSecret(vault_uri, "euskalmet-fingerprint")
priv_key = mssparkutils.credentials.getSecret(vault_uri, "euskalmet-priv-key")

print("Euskalmet fingerprint retrieved:", fingerprint[:8] + "…" if fingerprint else "(empty)")
print("Euskalmet private key retrieved:", "yes" if priv_key else "(empty)")
print("Secrets retrieved successfully")

# CELL 2
#
# Bronze — Euskalmet ingestion
#
# Ingest raw Euskalmet data into the Bronze Delta layer as managed tables
# (append-only, audit-stamped JSON payloads).
#
# Sources (CMDB.md §2.1):
# - EP-EUS-08 `stations_readings`  -> euskalmet_station_readings  (TBL-01)
# - EP-EUS-05 `weather_region_zone` + EP-EUS-04 location forecast
#   -> euskalmet_forecast_region                               (TBL-02)
#
# Auth: RS256 JWT signed with the private key held in fabric-meteoseek-kv
# (secret `euskalmet-priv-key`, fingerprint in `euskalmet-fingerprint`).
#
# NOTE: no dbutils in Fabric — use plain Python variables / notebook parameters.

# Global settings (Fabric: replace with notebook/pipeline parameters as needed)
lakehouse = "Demo"                          # Lakehouse name
euskalmet_base_url = "https://api.euskadi.eus"
region_id = ""                              # resolve via geolocations_regions
zone_id = ""                                # resolve via geolocations_zone
location_id = ""                            # resolve via geolocations_locations

# CELL 3
#
# Imports + shared helpers

import json
import sys
import urllib.request

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# importing common library helper functions
sys.path.insert(0, "/lakehouse/default/Files/meteoseek/common")
from fabric_utils import (
    euskalmet_headers,
    lakehouse_paths,
    write_bronze,
)

paths = lakehouse_paths(lakehouse)
base_url = euskalmet_base_url.rstrip("/")

# CELL 4
#
# Auth — build the RS256 JWT from the Key Vault private key + fingerprint

if not priv_key:
    raise RuntimeError("Missing secret 'euskalmet-priv-key'. See CMDB.md SEC-01.")

headers = euskalmet_headers(priv_key, fingerprint=fingerprint)

# CELL 5
#
# Pull station readings (EP-EUS-08)


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

# CELL 6
#
# Pull region-zone forecast (EP-EUS-05, fallback EP-EUS-04)

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

# CELL 7
#
# Write Bronze (managed tables; append-only, idempotent via payload hash)

# TBL-01 — station readings.
if readings_df.count() > 0:
    readings_df = readings_df.withColumn(
        "payload_hash", F.sha2(F.col("payload"), 256)
    )
    write_bronze(
        spark,
        readings_df,
        "euskalmet_station_readings",
        dedupe_cols=["payload_hash"],
    )
    print(f"Wrote {readings_df.count()} station readings to Bronze.")

# TBL-02 — forecast. Dedupe on (source_ep, payload_hash).
if forecast_df is not None and forecast_df.count() > 0:
    forecast_df = forecast_df.withColumn("payload_hash", F.sha2(F.col("payload"), 256))
    write_bronze(
        spark,
        forecast_df,
        "euskalmet_forecast_region",
        dedupe_cols=["source_ep", "payload_hash"],
    )
    print(f"Wrote {forecast_df.count()} forecast rows to Bronze.")
else:
    print("No forecast payload returned; region/zone/location IDs may be unset.")

# CELL 8
#
# Sanity check the managed tables

df = spark.sql("SELECT * FROM Demo.dbo.euskalmet_station_readings LIMIT 1000")
display(df)

df = spark.sql("SELECT * FROM Demo.dbo.euskalmet_forecast_region LIMIT 1000")
display(df)
