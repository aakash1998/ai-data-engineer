#!/usr/bin/env python3
"""
snapshot.py — writes dashboard_snapshot.json at the project root.

Consumed by the dashboard artifact. Contents:
  - recent_runs: last 10 run records (run_id, started_at, duration_s,
    rows_in, rows_silver, rows_gold, tests_*, quarantined, status)
  - open_incidents: recent incidents (severity warning and above), newest first
  - freshness: per-source newest-data age
  - samples: 10 sample rows per gold mart (for charts)
  - generated_at
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
from build import get_con  # noqa: E402

WAREHOUSE_DB = PROJECT_ROOT / "warehouse" / "warehouse.duckdb"
RUNS_DIR = PROJECT_ROOT / "runs"


def read_jsonl(path: Path, limit: int | None = None) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows[-limit:] if limit else rows


def df_to_records(con, sql: str) -> list[dict]:
    try:
        rel = con.execute(sql)
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, [str(v) for v in row])) for row in rel.fetchall()]
    except Exception:
        return []


def write_snapshot() -> dict:
    con = get_con(WAREHOUSE_DB)
    runs = read_jsonl(RUNS_DIR / "run_log.jsonl", limit=10)
    incidents = [i for i in read_jsonl(RUNS_DIR / "incident_log.jsonl", limit=200)
                 if i.get("severity") in ("warning", "high", "critical")]
    incidents = incidents[-20:][::-1]

    freshness = {}
    for label, sql in (
        ("crypto", "SELECT MAX(snapshot_ts) FROM silver_crypto"),
        ("weather", "SELECT MAX(ingested_at) FROM silver_weather"),
    ):
        try:
            val = con.execute(sql).fetchone()[0]
            if val is None:
                freshness[label] = {"newest": None, "age_hours": None}
            else:
                age_h = (datetime.now(timezone.utc)
                         - val.replace(tzinfo=timezone.utc)).total_seconds() / 3600
                freshness[label] = {"newest": str(val), "age_hours": round(age_h, 2)}
        except Exception:
            freshness[label] = {"newest": None, "age_hours": None}

    samples = {
        "gold_crypto_daily": df_to_records(
            con, "SELECT * FROM gold_crypto_daily ORDER BY market_date DESC, "
                 "avg_market_cap DESC NULLS LAST LIMIT 10"),
        "gold_weather_hourly": df_to_records(
            con, "SELECT * FROM gold_weather_hourly ORDER BY hour_ts DESC LIMIT 10"),
    }
    tables = {}
    for t in ("bronze_crypto", "bronze_weather", "silver_crypto", "silver_weather",
              "gold_crypto_daily", "gold_weather_hourly",
              "quarantine_crypto", "quarantine_weather"):
        try:
            tables[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            tables[t] = 0
    con.close()

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "recent_runs": runs,
        "open_incidents": incidents,
        "freshness": freshness,
        "samples": samples,
        "table_counts": tables,
    }
    out = PROJECT_ROOT / "dashboard_snapshot.json"
    with open(out, "w") as f:
        json.dump(snapshot, f, indent=2)
    print(f"[snapshot] wrote {out}")
    return snapshot


if __name__ == "__main__":
    write_snapshot()
