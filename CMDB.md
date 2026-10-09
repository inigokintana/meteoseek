# CMDB — MeteoSeek

**Configuration Management Database.** Single source of truth for every configuration item (CI) in the MeteoSeek system. Keep this file authoritative: update it in the same commit as any infrastructure, credential, or schema change.

> Companion to `README.md` (architecture & rationale). This file holds the *what/where/who*, not the *why*.

---

## 1. Workspaces & Environments

| CI ID | Type | Name | Value / Notes | Owner |
|---|---|---|---|---|
| ENV-01 | Azure Fabric Spark Settings 2.0 (Spark 4.1 Delta 4.2)  Workspace | `DEMO` | Landing zone for all Bronze/Silver/Gold data. OneLake + Power BI + Pipelines. | Data Eng. |
| ENV-02 | Region | West Europe | Assumed Azure region (confirm). | Infra |
| ENV-03 | Tenant / Capacity | F16 for now | Fabric capacity SKU + tenant ID. | Infra |

---

## 2. Data Sources & Endpoints

### 2.1 Euskalmet (Open Data Euskadi)

| CI ID | Item | Value |
|---|---|---|
| SRC-EUS-01 | Portal / docs | https://opendata.euskadi.eus/euskalmet-api/-/euskalmet-en-apia/ |
| SRC-EUS-02 | Production base URL | `https://api.euskadi.eus` |
| SRC-EUS-03 | Sandbox base URL | `https://api.sandbox.euskadi.eus` |
| SRC-EUS-04 | Auth model | API key → RS256 JWT, `Authorization: Bearer <JWT>` |
| SRC-EUS-05 | JWT audience (`aud`) | `met01.apikey` |
| SRC-EUS-06 | JWT version | `1.0.0` |
| SRC-EUS-07 | API key / private key | Stored in Azure Key Vault (see SEC-02). **Not in repo.** |
| SRC-EUS-08 | Support contact | opendata@euskadi.eus |

**Endpoint catalogue (used by MeteoSeek):**

| CI ID | Endpoint | Purpose |
|---|---|---|
| EP-EUS-01 | `geolocations_regions` | Resolve region → region ID |
| EP-EUS-02 | `geolocations_zone` | Resolve zone → zone ID (Vitoria-Gasteiz) |
| EP-EUS-03 | `geolocations_locations` | Resolve location → location ID (city) |
| EP-EUS-04 | `weather_region_zone_location` | Location-level forecast (primary forecast source) |
| EP-EUS-05 | `weather_region_zone` | Zone-level forecast (fallback / broader) |
| EP-EUS-06 | `stations_station` | List of stations covering Vitoria-Gasteiz |
| EP-EUS-07 | `stations_measure` | Station measurements (historical) |
| EP-EUS-08 | `stations_readings` | Station readings (historical) |
| EP-EUS-09 | `stations_sensors` | Sensor-level readings |
| EP-EUS-10 | `alerts_forecast` | Weather alerts (model feature) |
| EP-EUS-11 | `astro_calendar` | Sunrise/sunset (daylight feature) |

### 2.2 Seeketing Observer REST API

| CI ID | Item | Value |
|---|---|---|
| SRC-SEK-01 | Base URL | `https://www6.siketing.com/observer/rest/` |
| SRC-SEK-02 | HTTP method | `POST` (`multipart/form-data`) |
| SRC-SEK-03 | Request shape | `method=<service>`, `params=<JSON string>` |
| SRC-SEK-04 | Auth service | `authRest` (`login`, `password`) → returns session `key` |
| SRC-SEK-05 | Vitoria-Gasteiz `app_id` | `20212021007` |
| SRC-SEK-06 | Credentials | Stored in Azure Key Vault (see SEC-02). **Not in repo.** |
| SRC-SEK-07 | Observer UI | https://www6.siketing.com/observer/ |

**Endpoint catalogue (used by MeteoSeek):**

| CI ID | Service | Returns | Used |
|---|---|---|---|
| EP-SEK-01 | `authRest` | session `key` | auth |
| EP-SEK-02 | `getAppZonesPublic` | zone list (id, name, nodes, status) | dim_zone |
| EP-SEK-03 | `getZoneVisitsH` | hourly zone visits / unique / recurrents / visit time | fact_footfall |
| EP-SEK-04 | `getVisitsHbyHoursInterval` | accumulated visits per interval | fact_footfall (aggregate) |
| EP-SEK-05 | `getSensorsData` / `getSensorsDataByType` / `getSensorDataH` | sensor detections / hourly | validation, granular |
| EP-SEK-06 | `getDwellTimeAverages` / `getDwellTimeData` | dwell time per zone | feature |
| EP-SEK-07 | `getQueueWaitingTime` | queue waiting per zone | feature |
| EP-SEK-08 | `getJourneysSummary` | inter-zone journeys | flow analysis |
| EP-SEK-09 | `getSankeyPathMatrixData` | zone path matrix | flow analysis |
| EP-SEK-10 | `getProfilesDetails` / `getProfileResults` | offline behavioural profiles | segmentation (optional) |

---

## 3. Secrets & Credentials

All secrets live in Azure Key Vault `fabric-meteoseek-kv`
(`https://fabric-meteoseek-kv.vault.azure.net/`) and are read by notebooks via
`mssparkutils.credentials.getSecret(vault_uri, secret_name)`.

| CI ID | Secret | Location | Accessors |
|---|---|---|---|
| SEC-01 | Euskalmet `euskalmet-priv-key` (RS256 JWT signing) | KV `fabric-meteoseek-kv` | Ingestion pipeline |
| SEC-01 | Euskalmet `euskalmet-pub-key` (public half) | KV `fabric-meteoseek-kv` | (verification) |
| SEC-01 | Euskalmet `euskalmet-fingerprint` (JWT `kid` header) | KV `fabric-meteoseek-kv` | Ingestion pipeline |
| SEC-02 | Seeketing `seeketing-login` / `seeketing-password` / `seeketing-app-id` | KV `fabric-meteoseek-kv` | Ingestion pipeline |
| SEC-03 | Seeketing session `key` (derived) | Never persisted; in-memory per run | Ingestion pipeline |
| SEC-04 | Hermes read-only connector credentials | Hermes secret store | Hermes Agent |

**Policy:** secrets never committed to the repo; rotation via Key Vault. No write credentials are ever exposed to the agent layer.

---

## 4. Data Model (Fabric tables)

| CI ID | Layer | Table / Object | Grain | Key | Source(s) |
|---|---|---|---|---|---|
| TBL-01 | Bronze | `euskalmet_station_readings` | raw JSON | — | EP-EUS-08 |
| TBL-02 | Bronze | `euskalmet_forecast_region` | raw JSON | — | EP-EUS-05/04 |
| TBL-03 | Bronze | `seeketing_zone_visits_h` | raw JSON | — | EP-SEK-03 |
| TBL-04 | Bronze | `seeketing_sensors_data` | raw JSON | — | EP-SEK-05 |
| TBL-05 | Silver | `fact_weather_hourly` | hour × location | `(location_id, ts_hour)` | TBL-01/02 |
| TBL-06 | Silver | `fact_footfall_hourly` | hour × zone | `(zone_id, ts_hour)` | TBL-03/04 |
| TBL-07 | Silver | `dim_zone` | zone | `zone_id` | EP-SEK-02 |
| TBL-08 | Silver | `dim_location` | location | `location_id` | EP-EUS-02/03 |
| TBL-09 | Gold | `fact_occupancy_hourly` | hour × zone × location | `(location_id, zone_id, ts_hour)` | TBL-05 ⋈ TBL-06 |
| TBL-10 | Gold | `fact_forecast` | forecast hour × location | `(location_id, ts_hour)` | EP-EUS-04 |
| TBL-11 | Gold | `dim_time` | hour | `ts_hour` | generated |

**Join key:** `(location_id / zone_id, ts_hour)` aligned on hourly grain (see README §3.3).

---

## 5. Zone / Station Mapping (Vitoria-Gasteiz)

| CI ID | Item | Value | Notes |
|---|---|---|---|
| MAP-01 | City | Vitoria-Gasteiz | Basque Country, Álava |
| MAP-02 | Seeketing `app_id` | `This is secret` | Deployment "Vitoria" |
| MAP-03 | Euskalmet location ID | (TBD — resolve via EP-EUS-03) | Fill after first geolocation pull |
| MAP-04 | Euskalmet zone ID | (TBD — resolve via EP-EUS-02) | Fill after first geolocation pull |
| MAP-05 | Weather stations covering city | (TBD — resolve via EP-EUS-06) | Nearest-station list |

---

## 6. Pipelines & Schedules

| CI ID | Pipeline | Trigger / Cadence | Source | Target | Status |
|---|---|---|---|---|---|
| PIP-01 | `ingest_euskalmet_forecast` | every 1h | EP-EUS-04 | Bronze → Silver `fact_forecast` | planned |
| PIP-02 | `ingest_euskalmet_historical` | daily backfill + 1h delta | EP-EUS-06/07/08 | Bronze → Silver `fact_weather_hourly` | planned |
| PIP-03 | `ingest_seeketing_footfall` | every 1h (respect consolidation via `getDataState`) | EP-SEK-03/04 | Bronze → Silver `fact_footfall_hourly` | planned |
| PIP-04 | `build_gold_occupancy` | after PIP-02 & PIP-03 | Silver | Gold `fact_occupancy_hourly` | planned |

---

## 7. Analytics & Agent

| CI ID | Item | Value |
|---|---|---|
| ANA-01 | Power BI workspace | `DEMO` (same Fabric workspace) |
| ANA-02 | Semantic model | Over Gold tables; Direct Lake mode |
| ANA-03 | Reports | Occupancy trend, weather↔footfall correlation, zone heatmap |
| AGT-01 | Agent runtime | Hermes Agent (Nous Research) |
| AGT-02 | Agent permissions | Read-only: semantic model + forecast endpoint. No Fabric write. |
| AGT-03 | Prediction tool | Combines `fact_forecast` (current) + historical correlation `fact_occupancy_hourly` |
| AGT-04 | Agent model / config | (TBD — provider + model) |

---

## 8. Ownership & Contacts

| CI ID | Role | Contact |
|---|---|---|
| OWN-01 | Data Engineering / Ingestion | (TBD) |
| OWN-02 | Analytics / Power BI | (TBD) |
| OWN-03 | Agent / AI | (TBD) |
| OWN-04 | Euskalmet support | opendata@euskadi.eus |
| OWN-05 | Seeketing support | (TBD) |

---

## 9. Open Items / TODOs

- [ ] Confirm Azure region (ENV-02) and Fabric capacity (ENV-03).
- [ ] Resolve Euskalmet location/zone IDs and station list for Vitoria-Gasteiz (MAP-03/04/05).
- [ ] Confirm Seeketing `app_id=20212021007` is the production Vitoria deployment.
- [ ] Define Gold table schemas (column types) and Power BI semantic model measures.
- [ ] Select Hermes agent model + provider (AGT-04).
- [ ] Define back-test window and success metric for prediction validation.
