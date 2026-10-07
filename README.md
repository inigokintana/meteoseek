# MeteoSeek

**Crossing Euskalmet meteorological data with Seeketing pedestrian/visitor data for the city of Vitoria-Gasteiz to forecast city occupancy.**

| | |
|---|---|
| **Project** | MeteoSeek |
| **City / Scope** | Vitoria-Gasteiz (Basque Country, Spain) |
| **Primary data sources** | Euskalmet (Open Data Euskadi) · Seeketing Observer REST API |
| **Target landing zone** | Azure Fabric — workspace `DEMO` |
| **Downstream analytics** | Power BI reports · Hermes Agent conversational predictions |
| **Status** | DEMO / Proof-of-Concept |

---

## 1. Purpose

MeteoSeek answers a single business question with a repeatable data pipeline:

> **"How many people will be in Vitoria-Gasteiz (in aggregate and per zone) over the next N hours/days, given the Euskalmet weather forecast and the historical relationship between weather and Seeketing-measured footfall?"**

To answer it, the project ingests two independent, complementary datasets and correlates them:

1. **Euskalmet** — authoritative Basque meteorological data: historical observations (station readings/sensors) and forecasts (region / region-zone / location-level).
2. **Seeketing** — anonymous, device-based occupancy and flow measurement for the Vitoria-Gasteiz deployment: zone visits, unique visitors, dwell time, queue waiting times, and journey paths.

The correlation is stored centrally in **Azure Fabric (`DEMO` workspace)**, exposed to **Power BI** for reporting, and served conversationally by a **Hermes Agent** for ad-hoc predictions.

---

## 2. The two data sources

### 2.1 Euskalmet (Open Data Euskadi)

- **Portal / docs:** https://opendata.euskadi.eus/euskalmet-api/-/euskalmet-en-apia/
- **Auth model:** API key → generate a **JWT (RS256)** signed with your private key, send as `Authorization: Bearer <JWT>`. JWT claims include `aud: met01.apikey`, `iss`, `exp`, `version: 1.0.0`, `iat`, `email`.
- **Endpoints (production):** `https://api.euskadi.eus` · **Sandbox:** `https://api.sandbox.euskadi.eus`

| Domain | Endpoint | Relevance to MeteoSeek |
|---|---|---|
| Geo | `geolocations_regions`, `geolocations_zone`, `geolocations_locations`, `geolocations_basins` | Resolve Vitoria-Gasteiz to canonical zone/location IDs used by forecast + readings |
| Forecast | `weather_region`, `weather_region_zone`, `weather_region_zone_location` | Forecast variables (temp, precip, wind, cloud) at city granularity |
| Historical | `stations_station`, `stations_measure`, `stations_readings`, `stations_sensors` | Observed historical meteo at stations covering the city |
| Supporting | `alerts_forecast`, `ocean_forecast`, `radar`, `astro_calendar`, `astro_tides`, `maps_*` | Context features (alerts, sunlight hours) useful as model features |

### 2.2 Seeketing Observer REST API

- **Base URL:** `https://www6.siketing.com/observer/rest/`
- **Method:** HTTP `POST`, `multipart/form-data` with two fields: `method` (service name) and `params` (JSON string).
- **Auth:** `authRest` with `login` / `password` returns a session `key`; all subsequent calls pass `key` + `app_id`.
- **Vitoria-Gasteiz `app_id`:** `is secret do not show it` (deployment "Vitoria").

| Group | Service | Returns |
|---|---|---|
| Configuration | `getAppZonesPublic`, `getDataState` | Zone list for the location; data consolidation state |
| Occupancy & Visits | `getZoneVisitsH`, `getVisitsHbyHoursInterval` | Hourly zone visits, unique/recurrent visitors, visit time; accumulated visits per time interval |
| Behavior / Profiles | `getVisitsNodesTimes` (loyalty), `getProfilesDetails`, `getProfileResults` | Loyalty visits; offline behavioural profiles |
| Sensors | `getAppZonesDetailed`, `getSensorsData`, `getSensorsDataByType`, `getSensorDataH` | Sensor-level raw detections and hourly aggregates |
| Queue / Dwell | `getDwellTimeData`, `getQueueWaitingTime`, `getDwellTimeAverages` | Dwell time and queue waiting metrics per zone |
| Journeys / Paths | `getJourneysSummary`, `getSankeyPathMatrixData` (`getSankeyPathData` deprecated) | Inter-zone journey flows and path matrices |

---

## 3. Why land the data in Azure Fabric (workspace `DEMO`)

### 3.1 The problem Fabric solves

Both sources are **external REST APIs with distinct auth models** (Euskalmet JWT, Seeketing session key) and **different cadences and shapes** (forecast vs. historical; hourly zone readings vs. station sensor streams). Naively pulling them on-demand into a BI tool would mean:

- Re-authenticating and re-pulling on every report refresh (slow, fragile, rate-limit exposure).
- No single join key between "weather at this hour" and "footfall in that zone".
- No reusable, versioned history — the raw APIs are point-in-time and, for Seeketing, subject to consolidation windows.

### 3.2 Why Fabric specifically

| Benefit | Rationale |
|---|---|
| **OneLake is the single store** | Both datasets land as Delta-Parquet in OneLake; Power BI reads them directly via shortcuts without duplication. |
| **Native Bronze → Silver → Gold** | Raw JSON (Bronze) → typed/cleaned tables with a common temporal+spatial key (Silver) → aggregated fact/dimension model for BI (Gold). |
| **Serverless + Spark pipelines** | Scheduled Notebooks / Data Pipelines pull both APIs, normalise, and join — no separate ETL cluster to operate. |
| **Power BI native integration** | Semantic models and Direct Lake reports sit in the same workspace — zero-copy, always-fresh. |
| **Managed secrets** | Seeketing key and Euskalmet private key live in Fabric/Key Vault, not in notebooks or agent code. |
| **Single governance perimeter** | Row/column-level security, data lineage (Purview), and RBAC apply to one governed copy rather than two ad-hoc integrations. |
| **Reproducible history** | Delta tables keep append-only history of both weather and footfall → time-series models can be retrained from a stable, auditable base. |

### 3.3 The data model (conceptual)

```
Bronze (raw)                     Silver (typed, keyed)                 Gold (BI-ready)
─────────────────────────────   ────────────────────────────────────  ────────────────────────────
euskalmet_station_readings   →  fact_weather_hourly                      dim_zone
euskalmet_forecast_region    →  (location_id, ts_hour, temp_c,           fact_occupancy_hourly
euskalmet_station_sensors        precip_mm, wind_kmh, cloud_pct, ...)    fact_forecast (weather) ─┐
                                                                                                 ├─ JOIN on
seeketing_zone_visits_h      →  fact_footfall_hourly                    dim_time                  │  (location_id,
seeketing_sensors_data       →  (zone_id, ts_hour, visits,                                   ─────┘   ts_hour)
seeketing_dwell / queue          visitors_unique, recurrents,
                                 visit_time_avg, dwell, queue_wait)
```

The **join key** is `(location_id / zone_id, ts_hour)` — the city/zone resolved from Euskalmet geolocation data and the Seeketing zone map, aligned on an hourly time grain. This single keyed Silver layer is what makes both Power BI reporting *and* the Hermes Agent prediction trivial downstream.

---

## 4. Why use a Hermes Agent to interact with the user

### 4.1 The gap the agent fills

Power BI dashboards answer *"what happened"* and *"what is the trend"*. They do not answer, in natural language, *"should I staff more people in zone 3 tomorrow afternoon?"* — because that question requires **joining the forecast with the historical weather→footfall relationship and reasoning about it**, not just plotting two lines.

A **Hermes Agent** (Nous Research) closes that loop:

1. It reads the **Gold model in Fabric** (or the semantic model Power BI already uses) through read-only connectors.
2. It pulls the **current Euskalmet forecast** for Vitoria-Gasteiz.
3. It applies the **historical correlation** learned from `fact_weather_hourly ⋈ fact_footfall_hourly` to produce a prediction.
4. It returns the answer conversationally, with the reasoning and the confidence, and can be asked follow-up ("what if it rains all day?") without a new report.

### 4.2 Why an agent, not a hard-coded script or a static ML endpoint

| Approach | Limitation | Agent advantage |
|---|---|---|
| Static Power BI report | Fixed charts, no dialogue, can't combine forecast with arbitrary "what-if" | Conversational, composable, ad-hoc |
| Hard-coded prediction script | One fixed model/query; re-deploy for every question | Agent composes tools, queries, and models per request |
| Single ML endpoint | Returns one number, no explanation, no access to context | Agent explains reasoning, cites source data, adapts scope |
| Manual analyst | Slow, non-repeatable, doesn't scale | Instant, repeatable, auditable |

### 4.3 Why Hermes specifically

- **Tool orchestration** — Hermes drives the read-only tools (Fabric SQL/Semantic model, REST pulls, and the predictive model) behind a governed interface, so the user never touches raw APIs or keys.
- **Reasoning over evidence** — it can state *which* historical period and *which* weather features drove the prediction, not just emit a number.
- **Least-privilege by design** — the agent is granted read-only access to the Gold layer and the forecast endpoint; it holds no write access to Fabric, matching the Zero-Trust posture this project assumes.
- **One conversational surface** — the same agent can answer *"how was footfall last Saturday"* (historical) and *"how will it be next Saturday"* (forecast-driven) without the user switching tools.

### 4.4 Example interaction

> **User:** "How many people should I expect in Vitoria-Gasteiz city centre tomorrow between 16:00 and 20:00?"
>
> **Hermes:** pulls the Euskalmet zone forecast for that window (e.g. 19°C, 0.4 mm rain, 12 km/h wind), looks up the historical footfall for comparable conditions on comparable weekday/hours from the Gold model, and answers with a predicted range and the confidence interval, citing the weather features and the baseline.

---

## 5. Security & posture (Zero-Trust)

- **Least privilege:** Hermes agent → read-only on Gold layer + forecast endpoint; ingestion service → scoped write to its Bronze/Silver paths only.
- **Secrets:** Euskalmet JWT private key and Seeketing credentials stored in Azure Key Vault / Fabric-managed secrets, referenced by the pipeline, never committed to source.
- **Anonymous data:** Seeketing visitor data is device-ID anonymised; no PII is ingested or persisted.
- **Network:** outbound-only ingestion; no inbound exposure of the Fabric workspace beyond RBAC.

---

## 6. High-level architecture

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                  Azure Fabric — workspace "DEMO"            │
                    │                                                             │
  Euskalmet API ───▶│  Data Pipeline (scheduled)  ──▶  Bronze (Delta)             │
  (JWT Bearer)      │                                    │  normalize + key      │
                    │                                    ▼                        │
  Seeketing API ───▶│  Data Pipeline (scheduled)  ──▶  Silver (typed) ──▶ Gold    │
  (session key)     │                                                             │
                    │        ┌──────────────────────────┐        ┌──────────────┐ │
                    │        │ Power BI (Direct Lake)     │        │ Semantic     │ │
                    │        │  dashboards / reports      │        │  model       │ │
                    │        └──────────────────────────┘        └──────┬───────┘ │
                    └───────────────────────────────────────────────────┼─────────┘
                                                                         │ read-only
                                                        ┌────────────────▼─────────┐
                                                        │   Hermes Agent           │
                                                        │  tools: semantic model,  │
                                                        │  forecast pull, model    │
                                                        │  ──▶ natural-language    │
                                                        │      occupancy prediction│
                                                        └──────────────────────────┘
                                                                         │
                                                                   user (chat)
```

---

## 7. Getting started (outline)

1. **Credentials** — obtain Euskalmet API key + private key (generate JWT); obtain Seeketing `login`/`password` and confirm `app_id=20212021007`.
2. **Fabric** — create `DEMO` workspace; register secrets in Key Vault; deploy Bronze/Silver/Gold notebooks and schedules.
3. **Semantic model** — build the Power BI semantic model over Gold; validate the `(location_id, ts_hour)` join.
4. **Hermes Agent** — grant read-only connectors to the semantic model + forecast endpoint; define the prediction tool and its reasoning prompt.
5. **Validate** — back-test predicted vs. actual footfall over a known historical window.

> See `CMDB.md` for the authoritative configuration-item inventory (credentials locations, endpoint catalogue, zone/station mapping, and ownership).
