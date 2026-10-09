# Fabric — MeteoSeek medallion pipelines

PySpark notebooks that land Euskalmet + Seeketing data into the Azure Fabric Spark Settings 2.0 (Spark 4.1 Delta 4.2)
`DEMO` workspace using a Bronze → Silver → Gold medallion architecture.

Everything here is plain Python (`.py`) written in the Databricks notebook
format so it can be imported directly into Fabric as a **Notebook** or run from
a **Data Pipeline / Spark Job Definition**. No secrets are embedded — all
credentials come from the Fabric secret store (fronts Azure Key Vault).

See the project root `CMDB.md` (authoritative CI inventory) and `README.md`
(architecture rationale) for the "what/why".

---

## Layout

```
Fabric/
├── common/
│   └── fabric_utils.py          # shared: secrets, JWT, Seeketing auth, Delta helpers
├── bronze/
│   ├── ingest_euskalmet.py      # station readings + region forecast -> Bronze (TBL-01/02)
│   └── ingest_seeketing.py      # zones + zone visits + sensors   -> Bronze (TBL-03/04)
├── silver/
│   ├── silver_weather.py        # -> fact_weather_hourly (TBL-05), dim_location (TBL-08)
│   └── silver_footfall.py       # -> fact_footfall_hourly (TBL-06), dim_zone (TBL-07)
├── gold/
│   └── gold_occupancy.py        # -> fact_occupancy_hourly (TBL-09), fact_forecast (TBL-10), dim_time (TBL-11)
└── README.md                    # this file
```

### Data-model mapping (from CMDB.md §4)

| CI ID   | Layer  | Table                      | Key                            | Notebook               |
|---------|--------|----------------------------|--------------------------------|------------------------|
| TBL-01  | Bronze | `euskalmet_station_readings` | (payload_hash)               | ingest_euskalmet       |
| TBL-02  | Bronze | `euskalmet_forecast_region`   | (source_ep, payload_hash)   | ingest_euskalmet       |
| TBL-03  | Bronze | `seeketing_zone_visits_h`     | (payload_hash)              | ingest_seeketing       |
| TBL-04  | Bronze | `seeketing_sensors_data`      | (payload_hash)              | ingest_seeketing       |
| TBL-05  | Silver | `fact_weather_hourly`         | (location_id, ts_hour)      | silver_weather         |
| TBL-06  | Silver | `fact_footfall_hourly`        | (zone_id, ts_hour)          | silver_footfall        |
| TBL-07  | Silver | `dim_zone`                    | zone_id                      | silver_footfall        |
| TBL-08  | Silver | `dim_location`                | location_id                  | silver_weather         |
| TBL-09  | Gold   | `fact_occupancy_hourly`       | (location_id, zone_id, ts_hour) | gold_occupancy      |
| TBL-10  | Gold   | `fact_forecast`               | (location_id, ts_hour)       | gold_occupancy         |
| TBL-11  | Gold   | `dim_time`                    | ts_hour                      | gold_occupancy         |

> `dim_location` (TBL-08) is not yet materialized in `silver_weather.py` — it
> requires the Euskalmet geolocation pull (EP-EUS-02/03) to resolve Vitoria-Gasteiz
> to a canonical `location_id`. It is a pending open item (CMDB.md §9, MAP-03/04).

---

## Prerequisites

1. **Fabric workspace `DEMO`** with a Lakehouse (the working Lakehouse name is
   `Demo`; bronze lands under `Files/meteoseek/bronze`, silver/gold as managed
   `Tables`). Override via the `lakehouse` variable in each notebook.
2. **Fabric capacity** (F16 per CMDB ENV-03).
3. **Secrets registered** in Azure Key Vault `fabric-meteoseek-kv` (see below).
4. **`requests` + `cryptography`** available on the Spark runtime. Fabric Spark
   runtimes ship both (the JWT is signed with `cryptography`, so no PyJWT
   dependency is required).

### Required Key Vault secrets (CMDB §3)

All secrets live in Azure Key Vault `fabric-meteoseek-kv` and are read by
notebooks via `mssparkutils.credentials.getSecret(vault_uri, secret_name)`.

| Secret name             | CI ID  | Content                                  |
|-------------------------|--------|------------------------------------------|
| `euskalmet-priv-key`    | SEC-01 | RS256 private key (PEM) for JWT signing  |
| `euskalmet-pub-key`     | SEC-01 | RS256 public key (PEM, verification)     |
| `euskalmet-fingerprint` | SEC-01 | Key fingerprint (JWT `kid` header)       |
| `seeketing-login`       | SEC-02 | Seeketing Observer login                 |
| `seeketing-password`    | SEC-02 | Seeketing Observer password              |
| `seeketing-app-id`      | SEC-02 | Seeketing Vitoria deployment `app_id`    |

The notebooks read these directly from Key Vault (do NOT register them as
Fabric workspace secrets — the `vault_uri` is referenced inline).

---

## Importing `fabric_utils` into notebooks

Fabric notebooks do not share a Python path by default. Two supported options:

**Option A — Lakehouse `Files` (recommended):** upload
`common/fabric_utils.py` to `Demo/Files/meteoseek/common/fabric_utils.py`,
then in each notebook prepend:

```python
import sys
sys.path.insert(0, "/lakehouse/default/Files/meteoseek/common")
from fabric_utils import ...
```

**Option B — Notebook resources:** attach `fabric_utils.py` as a notebook
resource in the Fabric UI and import directly (`import fabric_utils`).

Every notebook already contains the `sys.path.insert(...)` line for Option A.

---

## Orchestration order

Run (or schedule) in this dependency order:

```
1. bronze/ingest_euskalmet.py
2. bronze/ingest_seeketing.py
3. silver/silver_weather.py      (needs 1)
4. silver/silver_footfall.py     (needs 2)
5. gold/gold_occupancy.py        (needs 3 + 4)
```

Schedules (CMDB §6):

| CI ID | Pipeline                        | Cadence                              |
|-------|---------------------------------|--------------------------------------|
| PIP-01| ingest_euskalmet_forecast       | every 1h                             |
| PIP-02| ingest_euskalmet_historical     | daily backfill + 1h delta            |
| PIP-03| ingest_seeketing_footfall       | every 1h (respect `getDataState`)    |
| PIP-04| build_gold_occupancy            | after PIP-02 & PIP-03                |

In the Fabric UI, create a **Data Pipeline** with these activities:
1. `Notebook` → ingest_euskalmet
2. `Notebook` → ingest_seeketing
3. `Notebook` → silver_weather
4. `Notebook` → silver_footfall
5. `Notebook` → gold_occupancy

Chain them with `On success` connectors and set the schedule on the pipeline.

---

## Idempotency & re-runs

- **Bronze** is append-only, de-duplicated on a SHA-256 of the raw payload
  (`payload_hash`) plus `ingested_at` audit stamp. Re-running the same window
  will not duplicate rows.
- **Silver / Gold** use Delta `MERGE` (upsert) keyed on the grain columns, so
  re-runs converge to the same state (no duplicate keys).
- Seeketing consolidation: before a backfill, call `getDataState` to avoid
  pulling a not-yet-consolidated window (PIP-03 note). This is left as a
  pre-check to wire in once the deployment cadence is confirmed.

---

## Configuring notebook parameters

Fabric has no `dbutils.widgets` — notebooks use plain Python variables set at
the top of each file. At minimum set:

- `lakehouse` — Lakehouse name (working value `Demo`)
- `location_id` / `zone_id` / `region_id` — resolved from the first Euskalmet
  geolocation pull (CMDB MAP-03/04, currently TBD)
- `app_id` / `my_app_id` — Seeketing Vitoria deployment (read from KV secret
  `seeketing-app-id`; CMDB SRC-SEK-05 / MAP-02)

For scheduled runs, wire these as Fabric pipeline parameters or notebook
parameters so the schedule passes them through.

---

## Verification checklist

- [ ] KV secrets `euskalmet-priv-key` / `euskalmet-pub-key` / `euskalmet-fingerprint`,
      `seeketing-login` / `seeketing-password` / `seeketing-app-id` present
- [ ] `fabric_utils.py` uploaded to `Files/meteoseek/common/`
- [ ] Bronze notebooks run clean and write raw tables (`euskalmet_*`, `seeketing_*`)
- [ ] Silver notebooks produce typed `fact_weather_hourly` / `fact_footfall_hourly`
- [ ] Gold produces `fact_occupancy_hourly`, `fact_forecast`, `dim_time`
- [ ] Pipeline scheduled and ordered Bronze → Silver → Gold
- [ ] `location_id` / `zone_id` resolved (close CMDB open item MAP-03/04)
