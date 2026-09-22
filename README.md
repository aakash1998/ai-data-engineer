# Hired: An AI Data Engineer

A real, working data engineering project — not a demo with fake data.

**The concept:** an AI data engineer was "hired" and given a real job. It ingests
live data from public APIs on a schedule, builds a medallion-architecture
warehouse (bronze → silver → gold) with dbt, runs a data quality suite on every
build, quarantines bad rows instead of silently passing, pages on-call (an
incident log) when things break, and survives chaos drills the way a
production pipeline should. Every run, test result, and incident is logged and
surfaced on a live dashboard.

No mock data. Ever. The only synthetic rows in this repo live in clearly
labeled `drill-*` sandbox runs, and they never touch the real warehouse.

---

## Architecture

```
┌─────────────┐      ┌─────────────┐
│ CoinGecko   │      │ Open-Meteo  │
│ top-100     │      │ 20 cities,  │
│ crypto      │      │ hourly temp │
└──────┬──────┘      └──────┬──────┘
       │ 1 call/run         │ 1 batched call/run
       ▼                    ▼
┌─────────────────────────────────────┐
│ BRONZE (immutable JSONL on disk)    │
│ warehouse/bronze/<source>/<run>.jsonl│
│ + bronze_crypto / bronze_weather    │  ← append-only, idempotent loads
│   (DuckDB tables mirroring JSONL)   │    (run_id already loaded → skip)
└──────────────────┬──────────────────┘
                   │ dbt build (tag:silver)
                   ▼
┌─────────────────────────────────────┐
│ SILVER (dbt, full refresh)          │
│ silver_crypto  — deduped on         │
│   (coin_id, snapshot_ts), UTC       │
│   timestamps, surrogate keys       │
│ silver_weather — deduped on         │
│   (city, hour_ts)                   │
└──────────────────┬──────────────────┘
                   │ quality suite → quarantine → incidents
                   ▼
┌─────────────────────────────────────┐
│ GOLD (dbt, full refresh)            │
│ gold_crypto_daily  — per coin/day:  │
│   approx OHLC, avg volume, 24h chg  │
│ gold_weather_hourly — per city/hour │
│   + daily min/max context           │
└──────────────────┬──────────────────┘
                   │ quality suite (referential integrity)
                   ▼
┌─────────────────────────────────────┐
│ OBSERVABILITY                       │
│ runs/run_log.jsonl      per-run stats│
│ runs/incident_log.jsonl severity-    │
│   tagged incidents                   │
│ quarantine/             bad rows +   │
│   reason + run_id                    │
│ dashboard_snapshot.json → dashboard  │
└─────────────────────────────────────┘
```

Pipeline order per run (see `run_pipeline.py`):
1. **ingest** → bronze JSONL (immutable; `_run_id` + `_ingested_at` on every record)
2. **bronze load** → `bronze_*` tables (idempotent via `_etl_loaded_runs`)
3. **dbt `tag:silver`** → silver tables
4. **quality suite (silver)** → failures quarantined + incidents logged
5. **dbt `tag:gold`** → gold marts rebuilt on quarantined-clean silver
6. **quality suite (gold)** → referential integrity
7. **snapshot** → `dashboard_snapshot.json`

---

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Warehouse | **DuckDB** 1.5.5 (file: `warehouse/warehouse.duckdb`) | Zero-ops analytical DB, lives in the repo |
| Models | **dbt-core** 1.12.5 + **dbt-duckdb** 1.11.0 | The real-deal choice: versioned SQL, `ref()`/`source()` DAG, Jinja |
| Ingestion | Python + `requests`, retry w/ exponential backoff | 2 free keyless APIs, minimal call volume |
| Quality | Custom Python suite (`tests/quality.py`) | Finer control than dbt tests: quarantine + incidents |
| Scheduling | cron (see below) | Every 6h ingest; drills on demand |

dbt was chosen for the model layer because it installed cleanly (~2 min).
An early manifest-driven Python model runner was dropped to avoid shipping
two competing DAGs.

---

## Project structure

```
ai-data-engineer/
├── ingest/ingest.py          # Bronze ingestion (CoinGecko + Open-Meteo)
├── build.py                  # Bronze loader: JSONL → bronze_* (idempotent, schema-evolution safe)
├── run_pipeline.py           # Orchestrator: ingest → bronze → dbt silver → tests → dbt gold → tests → snapshot
├── chaos.py                  # Chaos drills (sandbox only, drill-* run_ids)
├── snapshot.py               # Writes dashboard_snapshot.json
├── tests/quality.py          # Data quality suite
├── models/
│   ├── sources.yml           # dbt sources: bronze_crypto, bronze_weather
│   ├── silver/silver_crypto.sql
│   ├── silver/silver_weather.sql
│   ├── gold/gold_crypto_daily.sql
│   └── gold/gold_weather_hourly.sql
├── dbt_project.yml / profiles.yml   # dev target = real warehouse; drill target = sandbox DBs
├── warehouse/
│   ├── bronze/crypto|weather/<run_id>.jsonl   # immutable raw (committed to git)
│   ├── sandbox/              # drill copies (git-ignored)
│   └── warehouse.duckdb      # the warehouse (git-ignored; rebuilt from bronze)
├── quarantine/               # quarantined rows as JSONL (reason + run_id)
├── runs/
│   ├── run_log.jsonl         # per-run stats
│   ├── incident_log.jsonl    # incidents (severity, check, details, run_id)
│   ├── <run_id>_tests.json   # full test results per run
│   └── drill_report.md       # chaos drill narrative + numbers
├── dashboard_snapshot.json   # what the dashboard artifact reads
└── README.md
```

---

## Setup

```bash
cd ai-data-engineer
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt        # duckdb, requests
.venv/bin/pip install "dbt-core>=1.8" "dbt-duckdb>=1.8"

# sanity checks
.venv/bin/python ingest/ingest.py --help
.venv/bin/dbt debug --project-dir . --profiles-dir .
```

### Run the pipeline

```bash
# full end-to-end run (ingest → bronze → dbt → tests → snapshot)
.venv/bin/python run_pipeline.py

# individual stages
.venv/bin/python ingest/ingest.py --run-id my-run --source all
.venv/bin/python build.py --db warehouse/warehouse.duckdb --run-id my-run
.venv/bin/dbt build --select tag:silver --project-dir . --profiles-dir .
.venv/bin/python tests/quality.py --db warehouse/warehouse.duckdb --run-id my-run --stage all
.venv/bin/dbt build --select tag:gold --project-dir . --profiles-dir .
.venv/bin/python snapshot.py
```

---

## Scheduling

Intended cadence: **every 6 hours** via cron (4 runs/day → ~4 crypto snapshots/day,
which is what makes the gold daily OHLC approximation meaningful):

```cron
# AI data engineer — pipeline run every 6h
0 */6 * * * cd /home/hatch/workspace/ai-data-engineer && .venv/bin/python run_pipeline.py >> runs/cron.log 2>&1
```

API budget per run: **2 HTTP calls total** (1 CoinGecko + 1 batched Open-Meteo) —
far under free-tier limits (CoinGecko ~10–30 calls/min).

---

## Data quality suite (`tests/quality.py`)

Runs after every build. Nothing silently passes.

| Check | What it does | On failure |
|---|---|---|
| `not_null` | `coin_id`, `current_price`, `total_volume` (crypto); `city`, `temperature_2m` (weather) | Rows → quarantine, incident (high) |
| `unique` | Natural keys `(coin_id, snapshot_ts)`, `(city, hour_ts)` in silver | Stale copies → quarantine, incident (high) |
| `accepted_values` | `vs_currency = 'usd'` | Rows → quarantine, incident (warning) |
| `freshness` | Crypto `max(snapshot_ts)` ≤ 12h old; weather feed ≤ 30h old | Incident (critical) — the SLA breach page |
| `row_count_anomaly` | Latest run's bronze count vs trailing-7-run avg; flag if > 40% deviation | Incident (warning); skipped until ≥ 2 runs exist |
| `referential_integrity` | Every gold row traceable to silver | Incident (high) |

**Quarantine policy:** offending rows are copied to `quarantine_<source>` tables
(plus a JSONL mirror in `quarantine/` with `quarantine_reason`, `quarantine_run_id`,
`quarantined_at`), then **deleted from silver** so gold builds on clean data only.
The pipeline keeps running — quarantine is a handling path, not a crash.

---

## Chaos drill (`chaos.py`)

```bash
.venv/bin/python chaos.py [--source-run-id RUN_ID]   # default: latest real bronze batch
```

Injects three realistic faults into **sandbox copies** of a real bronze batch.
Real bronze/silver/gold are never touched — each fault gets a fresh DuckDB file
(`warehouse/drill_*.duckdb`, git-ignored) and `drill-*` run_ids.

| Fault | Simulates | Result (latest drill) |
|---|---|---|
| **Duplicate delivery** — batch delivered twice in one payload, plus the loader invoked twice with the same run_id | A retried API request | 1st load 200 rows, 2nd load **0 rows** (idempotency); silver deduped to 100; uniqueness **pass** |
| **Null flood** — `total_volume` nulled on 30% of coins | An exchange feed glitch | `not_null` **failed** as designed; 30 rows quarantined; pipeline completed; gold built on 70 clean rows; high-severity incident logged |
| **Schema drift** — `total_volume` → `volume_24h` on 40% of records + a new field on 20% | An API change mid-day | Bronze absorbed new columns (schema evolution); silver `COALESCE` recovered 100/100 volumes; no hard failure; warning incident logged |

Full narrative with numbers: [`runs/drill_report.md`](runs/drill_report.md) —
this is the script for the LinkedIn animation.

---

## Dashboard snapshot (`dashboard_snapshot.json`)

Regenerated at the end of every pipeline run. The dashboard artifact reads this file.

```jsonc
{
  "generated_at": "...",
  "recent_runs": [  // last 10
    {"run_id": "...", "started_at": "...", "duration_s": 10.8,
     "rows_in": {"crypto": 100, "weather": 960},
     "rows_silver": {"crypto": 200, "weather": 960},
     "rows_gold": {"crypto_daily": 100, "weather_hourly": 960},
     "quarantined": 0,
     "tests_passed": 14, "tests_failed": 0, "tests_warned": 0,
     "status": "success"}  // success | partial
  ],
  "open_incidents": [  // severity warning+, newest first
    {"timestamp": "...", "run_id": "drill-...", "severity": "high",
     "check": "not_null:silver_crypto.total_volume", "details": "..."}
  ],
  "freshness": {
    "crypto": {"newest": "2026-09-22 15:37:10", "age_hours": 0.06},
    "weather": {"newest": "...", "age_hours": 0.0}
  },
  "samples": {
    "gold_crypto_daily": [ /* 10 rows, newest first */ ],
    "gold_weather_hourly": [ /* 10 rows, newest first */ ]
  },
  "table_counts": {"bronze_crypto": 200, ...}
}
```

---

## Honest notes

- **Real data only.** CoinGecko and Open-Meteo are live free APIs. If a source
  is down after 4 retries, the run records `status: failed/partial`, an incident
  is logged, and the freshness check flags the gap downstream. **No data is ever
  fabricated to fill a gap** — the gap itself is the scenario.
- **Drills are labeled.** Anything with a `drill-` run_id is a sandbox exercise.
  Drill quarantine files and incidents are kept (transparency) but never mixed
  into real tables.
- **Rate limits respected.** CoinGecko free tier is ~10–30 calls/min; we make
  exactly 1 per run. Open-Meteo is 1 batched call for all 20 cities per run.
- **dbt models are full-refresh**, not incremental. At this volume that's the
  right tradeoff (simpler, always idempotent); incremental models are the
  obvious next step if a source grows.
- **Gold crypto OHLC is approximate** — "open/high/low/close" are derived from
  ~4 snapshots/day, not tick data. Labeled as such in the model.
- **Freshness SLAs** (12h crypto / 30h weather) are chosen for a 6-hour cadence;
  retune if the schedule changes.
- **Not financial advice.** The crypto mart is pipeline demo data, not a
  trading signal.
