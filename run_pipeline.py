#!/usr/bin/env python3
"""
run_pipeline.py — end-to-end orchestrator.

Order (the realistic shape):
  1. ingest        -> bronze JSONL (immutable)
  2. load bronze   -> bronze_* tables (idempotent, Python)
  3. dbt build     -> silver_* tables (dbt, tag:silver)
  4. quality(silver) -> quarantine offenders, incidents logged
  5. dbt build     -> gold_* marts (dbt, tag:gold; built on quarantined-clean silver)
  6. quality(gold)  -> referential integrity
  7. snapshot      -> dashboard_snapshot.json + runs/run_log.jsonl

Usage:
    python run_pipeline.py [--run-id RUN_ID]
Exit code 0 = success (even with quarantines — those are handled, not fatal).
Exit code 2 = a source failed to ingest (gap logged; freshness will flag it).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
from build import get_con, load_bronze_file  # noqa: E402
from tests.quality import run_all as run_tests, log_incident  # noqa: E402

WAREHOUSE_DB = PROJECT_ROOT / "warehouse" / "warehouse.duckdb"
RUNS_DIR = PROJECT_ROOT / "runs"
DBT_BIN = PROJECT_ROOT / ".venv" / "bin" / "dbt"


def dbt_build(select: str, target: str = "dev",
              extra_env: dict | None = None) -> None:
    """Run `dbt build --select <select>` (models only; our quality suite is custom)."""
    env = dict(os.environ)
    if extra_env:
        env.update(extra_env)
    cmd = [str(DBT_BIN), "build", "--select", select, "--target", target,
           "--project-dir", str(PROJECT_ROOT),
           "--profiles-dir", str(PROJECT_ROOT)]
    print(f"[dbt] {' '.join(cmd[1:])}")
    proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env,
                          capture_output=True, text=True)
    tail = proc.stdout[-2500:]
    print(tail)
    if proc.returncode != 0:
        print(proc.stderr[-2500:], file=sys.stderr)
        raise RuntimeError(f"dbt build --select {select} failed")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def table_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return 0


def main() -> int:
    t0 = time.time()
    run_id = ("run-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
              + "-" + uuid.uuid4().hex[:8])
    print(f"[pipeline] run_id={run_id} started at {utc_now()}")

    # 1. ingest
    proc = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "ingest" / "ingest.py"),
         "--run-id", run_id],
        capture_output=True, text=True)
    print(proc.stdout)
    ingest_ok = proc.returncode == 0
    if proc.returncode not in (0, 2):
        print(proc.stderr, file=sys.stderr)
    if not ingest_ok:
        log_incident(run_id, "critical", "pipeline_ingest",
                     "one or more sources failed to ingest; downstream will show a freshness gap")

    # 2-3. bronze load (Python) + silver models (dbt)
    con = get_con(WAREHOUSE_DB)
    rows_in = {}
    rows_in["crypto"] = load_bronze_file(con, "crypto", run_id)
    rows_in["weather"] = load_bronze_file(con, "weather", run_id)
    con.close()
    dbt_build("tag:silver")

    # 4. quality on silver -> quarantine
    silver_results = run_tests(WAREHOUSE_DB, run_id, stage="silver")

    # 5. gold models on quarantined-clean silver (idempotent full refresh)
    dbt_build("tag:gold")

    # 6. quality on gold
    gold_results = run_tests(WAREHOUSE_DB, run_id, stage="gold")
    results = silver_results + gold_results

    # 7. run log + snapshot
    con = get_con(WAREHOUSE_DB)
    record = {
        "run_id": run_id,
        "started_at": utc_now(),
        "duration_s": round(time.time() - t0, 1),
        "rows_in": rows_in,
        "rows_silver": {"crypto": table_count(con, "silver_crypto"),
                        "weather": table_count(con, "silver_weather")},
        "rows_gold": {"crypto_daily": table_count(con, "gold_crypto_daily"),
                      "weather_hourly": table_count(con, "gold_weather_hourly")},
        "quarantined": table_count(con, "quarantine_crypto") + table_count(con, "quarantine_weather"),
        "tests_passed": sum(1 for r in results if r["status"] == "pass"),
        "tests_failed": sum(1 for r in results if r["status"] == "fail"),
        "tests_warned": sum(1 for r in results if r["status"] == "warn"),
        "tests_skipped": sum(1 for r in results if r["status"] == "skip"),
        "status": "success" if ingest_ok else "partial",
    }
    con.close()
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    with open(RUNS_DIR / "run_log.jsonl", "a") as f:
        f.write(json.dumps(record) + "\n")
    with open(RUNS_DIR / f"{run_id}_tests.json", "w") as f:
        json.dump({"run_id": run_id, "results": results}, f, indent=2)

    sys.path.insert(0, str(PROJECT_ROOT))
    from snapshot import write_snapshot
    write_snapshot()

    print(f"[pipeline] done in {record['duration_s']}s: "
          f"{record['tests_passed']} passed, {record['tests_failed']} failed, "
          f"{record['tests_warned']} warned — status={record['status']}")
    return 0 if ingest_ok else 2


if __name__ == "__main__":
    sys.exit(main())
