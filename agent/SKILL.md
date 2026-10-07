---
name: meteoseek-fabric-analyst
description: Use when answering questions about city occupancy, footfall, or weather in Vitoria-Gasteiz by reading the MeteoSeek Gold layer in the Azure Fabric DEMO workspace (read-only), and when producing weather-driven occupancy forecasts.
version: 1.0.0
author: MeteoSeek Data Engineering
license: Apache-2.0
platforms: [linux, macos, windows]
metadata:
  hermes:
    tags: [fabric, onelake, delta, weather, footfall, occupancy, forecasting, meteoseek]
    related_skills: [hermes-agent]
---

# MeteoSeek — Fabric analyst (read-only)

## Overview

This skill makes an agent a competent read-only analyst over the MeteoSeek data
model in Azure Fabric. The data describes the city of **Vitoria-Gasteiz**
(Basque Country): hourly weather (Euskalmet) crossed with hourly pedestrian
footfall (Seeketing), joined on `(location_id / zone_id, ts_hour)`.

The agent reads the **Gold layer** in the Fabric `DEMO` workspace (OneLake Delta
tables) plus the current Euskalmet forecast endpoint, and turns questions into
evidence-backed answers and reasoned predictions.

The authoritative data dictionary lives in the repo `CMDB.md` (§4 data model) and
`README.md` (§3.3). This skill is the *operational* layer on top of those.

## When to use

- User asks about occupancy / footfall / visitors in Vitoria-Gasteiz (historical or forecast).
- User asks "how many people will be … tomorrow / next N hours".
- User asks a weather↔footfall "what-if" ("what if it rains all day?").
- User asks to compare zones, days, or hours of the week.

Don't use for: writing/backfilling Fabric data (pipeline's job), exposing
credentials, or answering questions outside the MeteoSeek domain.

## Read-only contract (non-negotiable)

- Access is **read-only**: semantic model / SQL endpoint + forecast endpoint.
- **Never** expose a JWT, session key, or API key. Secrets are in Key Vault and
  are referenced only by the ingestion pipeline, never by you.
- **Never** run a write/merge/delete against Fabric. If a task seems to require
  it, stop and explain why it's out of scope.

## Data model (Gold layer, Fabric `DEMO`)

| Table                    | Grain / key                            | Meaning |
|--------------------------|----------------------------------------|---------|
| `fact_occupancy_hourly`  | `(location_id, zone_id, ts_hour)`      | observed footfall × weather per hour/zone |
| `fact_footfall_hourly`   | `(zone_id, ts_hour)`                   | hourly visits, uniques, dwell, queue |
| `fact_weather_hourly`    | `(location_id, ts_hour)`               | temp, precip, wind, cloud, humidity |
| `fact_forecast`          | `(location_id, ts_hour)`               | future weather projection |
| `dim_zone`               | `zone_id`                              | zone metadata (name, status, nodes) |
| `dim_location`           | `location_id`                          | location metadata |
| `dim_time`               | `ts_hour`                              | date, hour, day-of-week, is_weekend |

**Join key:** `(location_id / zone_id, ts_hour)` — the single keyed surface both
Power BI and the agent read from. `fact_occupancy_hourly` is the pre-joined
weather×footfall fact; prefer it for questions spanning both.

**Units (always state them):** temperature °C, precipitation mm, wind km/h,
cloud %, humidity %, counts are integers, dwell/queue in seconds.

## Query patterns

### Read the Gold tables (via SQL endpoint or semantic model)

Use the workspace's SQL analytics endpoint / semantic model. Equivalent Spark SQL:

```sql
-- Historical footfall in a zone over a window
SELECT zone_id, ts_hour, visits, visitors_unique
FROM fact_footfall_hourly
WHERE zone_id = <id>
  AND ts_hour BETWEEN '<start>' AND '<end>'
ORDER BY ts_hour;

-- Weather conditions in a window
SELECT ts_hour, temp_c, precip_mm, wind_kmh, cloud_pct
FROM fact_weather_hourly
WHERE location_id = <id>
  AND ts_hour BETWEEN '<start>' AND '<end>';

-- Occupancy (weather joined) for reporting
SELECT ts_hour, zone_id, visits, temp_c, precip_mm
FROM fact_occupancy_hourly
WHERE location_id = <id>
  AND ts_hour BETWEEN '<start>' AND '<end>';
```

### Forecast lookup

Read the current forecast for the target window:

```sql
SELECT ts_hour, temp_c, precip_mm, wind_kmh, cloud_pct
FROM fact_forecast
WHERE location_id = <id>
  AND ts_hour BETWEEN '<start>' AND '<end>';
```

Then find **comparable historical conditions** (same weekday, same hour band,
similar temperature/precipitation) in `fact_occupancy_hourly` and use them as the
baseline for the prediction.

## Answer recipe (forecast questions)

1. **Resolve scope** — which location/zone(s), which window (`ts_hour` range).
2. **Pull the forecast** for that window from `fact_forecast`.
3. **Pull comparable history** — filter `fact_occupancy_hourly` to same weekday
   + hour band, and weather within a tolerance of the forecast.
4. **State the prediction** as a **range with a confidence interval**, and name
   the weather features (temp, precip, wind) and the historical baseline that
   drove it.
5. **Cite** the table(s) and window used.

Example shape:

> "I expect roughly 1,800–2,400 people in the city centre tomorrow 16:00–20:00.
> The forecast is 19°C, 0.4 mm rain, 12 km/h wind; historically, comparable
> weekday evenings with light rain drew ~2,100 visitors (±15%)."

## Common pitfalls

1. **Fabricating a reading.** If a table is empty or the window is outside the
   forecast horizon, say "no data for that window" and why — don't extrapolate.
2. **Mixing grain.** Don't compare a zone-level number to a city-level number
   as if they were the same. `location_id` ≠ `zone_id`.
3. **Forgetting units.** Always attach °C / mm / km/h / % / count.
4. **Ignoring forecast horizon.** `fact_forecast` only covers the horizon the
   pipeline ingested; beyond it there is no projection — flag it.
5. **Stating a point estimate as fact.** Predictions are ranges; label them as
   such and give confidence.
6. **Reaching for a write.** Read-only, always. Refer writes to the pipeline.

## Verification checklist

- [ ] Source tables + time window cited in the answer
- [ ] Units stated on every measurement
- [ ] Forecast answers include a range + confidence + weather features + baseline
- [ ] No credential/secret appeared anywhere
- [ ] No write/merge/delete attempted against Fabric
- [ ] Grain respected (`location_id` vs `zone_id` not conflated)
