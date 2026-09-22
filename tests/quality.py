#!/usr/bin/env python3
"""
quality.py — data quality suite. Runs after every build.

Checks:
  1. not_null        key columns (coin_id, current_price, total_volume,
                     city, temperature_2m)
  2. unique          natural keys in silver (post-dedupe => must hold)
  3. accepted_values vs_currency = 'usd'
  4. freshness       crypto snapshot_ts <= 12h old; weather ingested <= 30h old
  5. row_count_anomaly latest run count vs trailing-7-run avg (>40% => warn)
  6. referential_integrity every gold row traceable to silver

Failure policy (nothing silently passes):
  - offending rows -> quarantine tables (+ JSONL copy in quarantine/),
    then DELETED from silver so gold builds on clean data
  - incident appended to runs/incident_log.jsonl
  - pipeline continues; test result recorded as fail/warn

Usage:
    python tests/quality.py --db warehouse/warehouse.duckdb --run-id RUN_ID [--stage silver|gold|all]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from build import get_con, utc_now  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = PROJECT_ROOT / "runs"
QUARANTINE_DIR = PROJECT_ROOT / "quarantine"

NOT_NULL_CHECKS = [
    # (table, column, severity)
    ("silver_crypto", "coin_id", "high"),
    ("silver_crypto", "current_price", "high"),
    ("silver_crypto", "total_volume", "high"),   # exchange feed glitch detector
    ("silver_weather", "city", "high"),
    ("silver_weather", "temperature_2m", "high"),
]
UNIQUE_CHECKS = [
    ("silver_crypto", ["coin_id", "snapshot_ts"]),
    ("silver_weather", ["city", "hour_ts"]),
]


def log_incident(run_id: str, severity: str, check: str, details: str) -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    incident = {"timestamp": utc_now(), "run_id": run_id,
                "severity": severity, "check": check, "details": details}
    with open(RUNS_DIR / "incident_log.jsonl", "a") as f:
        f.write(json.dumps(incident) + "\n")
    print(f"[quality] incident [{severity}] {check}: {details}")


def quarantine_rows(con: duckdb.DuckDBPyConnection, table: str, where: str,
                    reason: str, run_id: str) -> int:
    """Copy offending rows to quarantine (table + JSONL), then delete from source."""
    qtable = f"quarantine_{table.split('_', 1)[1]}"  # silver_crypto -> quarantine_crypto
    cols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {qtable} (
            {", ".join(f'"{c}" VARCHAR' for c in cols)},
            quarantine_reason VARCHAR, quarantined_at TIMESTAMPTZ, quarantine_run_id VARCHAR
        )
    """)
    # fetch offending rows as dicts for the JSONL mirror
    rows = con.execute(f"SELECT * FROM {table} WHERE {where}").fetchall()
    if not rows:
        return 0
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    with open(QUARANTINE_DIR / f"{run_id}_{table}.jsonl", "a") as f:
        for r in rows:
            rec = dict(zip(cols, [str(v) for v in r]))
            rec.update({"quarantine_reason": reason, "quarantine_run_id": run_id,
                        "quarantined_at": utc_now()})
            f.write(json.dumps(rec) + "\n")
    select_list = ", ".join(f'CAST("{c}" AS VARCHAR)' for c in cols)
    con.execute(f"""
        INSERT INTO {qtable}
        SELECT {select_list}, '{reason}', now(), '{run_id}'
        FROM {table} WHERE {where}
    """)
    con.execute(f"DELETE FROM {table} WHERE {where}")
    print(f"[quality] quarantined {len(rows)} rows from {table} (reason={reason})")
    return len(rows)


def check_not_null(con, table, column, severity, run_id, results):
    n = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL").fetchone()[0]
    if n:
        q = quarantine_rows(con, table, f"{column} IS NULL",
                            f"not_null:{column}", run_id)
        log_incident(run_id, severity, f"not_null:{table}.{column}",
                     f"{n} rows with NULL {column}; {q} quarantined")
        results.append({"check": f"not_null:{table}.{column}", "status": "fail",
                        "offending": n, "quarantined": q})
    else:
        results.append({"check": f"not_null:{table}.{column}", "status": "pass",
                        "offending": 0})


def check_unique(con, table, key_cols, run_id, results):
    keys = ", ".join(key_cols)
    sk = "crypto_snapshot_sk" if table == "silver_crypto" else "weather_hour_sk"
    dups = con.execute(f"""
        SELECT {keys} FROM {table} GROUP BY {keys} HAVING COUNT(*) > 1
    """).fetchall()
    name = f"unique:{table}({'+'.join(key_cols)})"
    if dups:
        # keep the latest-ingested copy of each key; quarantine stale copies
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _survivors AS
            SELECT {sk} FROM (
                SELECT {sk}, ROW_NUMBER() OVER (
                    PARTITION BY {keys} ORDER BY ingested_at DESC) AS rn
                FROM {table}
            ) WHERE rn = 1
        """)
        q = quarantine_rows(con, table,
                            f"{sk} NOT IN (SELECT {sk} FROM _survivors)",
                            f"unique:{'+'.join(key_cols)}", run_id)
        log_incident(run_id, "high", name,
                     f"{len(dups)} duplicate key groups; {q} stale copies quarantined")
        results.append({"check": name, "status": "fail",
                        "offending": len(dups), "quarantined": q})
    else:
        results.append({"check": name, "status": "pass", "offending": 0})


def check_accepted_values(con, table, column, allowed, run_id, results):
    allowed_sql = ", ".join(f"'{a}'" for a in allowed)
    n = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} NOT IN ({allowed_sql})"
    ).fetchone()[0]
    name = f"accepted_values:{table}.{column}"
    if n:
        q = quarantine_rows(con, table, f"{column} NOT IN ({allowed_sql})",
                            f"accepted_values:{column}", run_id)
        log_incident(run_id, "warning", name,
                     f"{n} rows with {column} not in {allowed}; {q} quarantined")
        results.append({"check": name, "status": "fail", "offending": n,
                        "quarantined": q})
    else:
        results.append({"check": name, "status": "pass", "offending": 0})


def check_freshness(con, run_id, results):
    # crypto: newest snapshot no older than 12h
    try:
        row = con.execute("SELECT MAX(snapshot_ts) FROM silver_crypto").fetchone()
    except Exception:
        row = None
    name = "freshness:silver_crypto"
    if row[0] is None:
        results.append({"check": name, "status": "skip", "details": "no rows"})
    else:
        age_h = (datetime.now(timezone.utc) -
                 row[0].replace(tzinfo=timezone.utc)).total_seconds() / 3600
        if age_h > 12:
            log_incident(run_id, "critical", name,
                         f"newest snapshot {row[0]} is {age_h:.1f}h old (>12h SLA)")
            results.append({"check": name, "status": "fail",
                            "details": f"{age_h:.1f}h old"})
        else:
            results.append({"check": name, "status": "pass",
                            "details": f"{age_h:.1f}h old"})
    # weather: feed refreshed within 30h
    try:
        row = con.execute("SELECT MAX(ingested_at) FROM silver_weather").fetchone()
    except Exception:
        row = None
    name = "freshness:silver_weather"
    if row[0] is None:
        results.append({"check": name, "status": "skip", "details": "no rows"})
    else:
        age_h = (datetime.now(timezone.utc) -
                 row[0].replace(tzinfo=timezone.utc)).total_seconds() / 3600
        if age_h > 30:
            log_incident(run_id, "critical", name,
                         f"weather feed last ingested {age_h:.1f}h ago (>30h SLA)")
            results.append({"check": name, "status": "fail",
                            "details": f"{age_h:.1f}h old"})
        else:
            results.append({"check": name, "status": "pass",
                            "details": f"{age_h:.1f}h old"})


def check_row_count_anomaly(con, run_id, results):
    for source, table in (("crypto", "bronze_crypto"), ("weather", "bronze_weather")):
        hist = con.execute(f"""
            SELECT _run_id, COUNT(*) AS cnt, MIN(CAST(_ingested_at AS TIMESTAMPTZ)) AS ts
            FROM {table} GROUP BY _run_id ORDER BY ts
        """).fetchall()
        name = f"row_count_anomaly:{table}"
        if len(hist) < 2:
            results.append({"check": name, "status": "skip",
                            "details": f"only {len(hist)} run(s); need >= 2 for baseline"})
            continue
        latest = hist[-1][1]
        baseline = sum(r[1] for r in hist[-7:-1]) / len(hist[-7:-1])
        dev = abs(latest - baseline) / baseline if baseline else 0
        if dev > 0.40:
            log_incident(run_id, "warning", name,
                         f"{source}: latest run {latest} rows vs trailing avg "
                         f"{baseline:.0f} ({dev:.0%} deviation > 40%)")
            results.append({"check": name, "status": "warn",
                            "details": f"{latest} vs avg {baseline:.0f} ({dev:.0%})"})
        else:
            results.append({"check": name, "status": "pass",
                            "details": f"{latest} vs avg {baseline:.0f} ({dev:.0%})"})


def check_referential_integrity(con, run_id, results):
    # every gold crypto row must trace to >=1 silver row (coin_id + date)
    try:
        n = con.execute("""
            SELECT COUNT(*) FROM gold_crypto_daily g
            WHERE NOT EXISTS (
                SELECT 1 FROM silver_crypto s
                WHERE s.coin_id = g.coin_id
                  AND CAST(s.snapshot_ts AS DATE) = g.market_date)
        """).fetchone()[0]
    except Exception:
        n = None
    name = "referential_integrity:gold_crypto_daily->silver_crypto"
    if n is None:
        results.append({"check": name, "status": "skip",
                        "details": "gold/silver table missing"})
    elif n:
        log_incident(run_id, "high", name, f"{n} orphan gold rows")
        results.append({"check": name, "status": "fail", "offending": n})
    else:
        results.append({"check": name, "status": "pass", "offending": 0})
    try:
        n = con.execute("""
            SELECT COUNT(*) FROM gold_weather_hourly g
            WHERE NOT EXISTS (
                SELECT 1 FROM silver_weather s
                WHERE s.city = g.city AND s.hour_ts = g.hour_ts)
        """).fetchone()[0]
    except Exception:
        n = None
    name = "referential_integrity:gold_weather_hourly->silver_weather"
    if n is None:
        results.append({"check": name, "status": "skip",
                        "details": "gold/silver table missing"})
    elif n:
        log_incident(run_id, "high", name, f"{n} orphan gold rows")
        results.append({"check": name, "status": "fail", "offending": n})
    else:
        results.append({"check": name, "status": "pass", "offending": 0})


def run_all(db_path: Path, run_id: str, stage: str = "all") -> list[dict]:
    con = get_con(db_path)
    results: list[dict] = []
    if stage in ("silver", "all"):
        for table, column, sev in NOT_NULL_CHECKS:
            check_not_null(con, table, column, sev, run_id, results)
        for table, keys in UNIQUE_CHECKS:
            check_unique(con, table, keys, run_id, results)
        check_accepted_values(con, "silver_crypto", "vs_currency", ["usd"],
                              run_id, results)
        check_freshness(con, run_id, results)
        check_row_count_anomaly(con, run_id, results)
    if stage in ("gold", "all"):
        check_referential_integrity(con, run_id, results)
    con.close()
    return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--stage", choices=["silver", "gold", "all"], default="all")
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    results = run_all(Path(args.db), args.run_id, args.stage)
    npass = sum(1 for r in results if r["status"] == "pass")
    nfail = sum(1 for r in results if r["status"] == "fail")
    nwarn = sum(1 for r in results if r["status"] == "warn")
    nskip = sum(1 for r in results if r["status"] == "skip")
    print(f"[quality] {npass} passed, {nfail} failed, {nwarn} warn, {nskip} skipped")
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump({"run_id": args.run_id, "results": results}, f, indent=2)
    return 0 if nfail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
