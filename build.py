#!/usr/bin/env python3
"""
build.py — bronze loader.

Appends a run's JSONL files into bronze_crypto / bronze_weather.
Idempotent: a run_id already present in the bronze table is skipped, so
re-loading the same file twice never duplicates bronze rows.
Schema-evolution safe: new columns are added (with a warning incident),
missing columns load as NULL. Raw values are never transformed.

The model layer (silver/gold) is owned by dbt — see dbt_project.yml.
dbt-core + dbt-duckdb installed cleanly in this environment (~2 min),
so dbt is the model runner. (An early manifest-driven Python runner was
dropped once dbt proved healthy, to avoid shipping two competing DAGs.)

Usage:
    python build.py --db warehouse/warehouse.duckdb --run-id RUN_ID
    python build.py --db warehouse/drill_x.duckdb --run-id drill-... --bronze-from warehouse/sandbox
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
BRONZE_DIR = PROJECT_ROOT / "warehouse" / "bronze"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_incident(run_id: str, severity: str, check: str, details: str) -> None:
    """Append an incident to the JSONL incident log. Never raises."""
    try:
        runs_dir = PROJECT_ROOT / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        incident = {"timestamp": utc_now(), "run_id": run_id,
                    "severity": severity, "check": check, "details": details}
        with open(runs_dir / "incident_log.jsonl", "a") as f:
            f.write(json.dumps(incident) + "\n")
        print(f"[build] incident [{severity}] {check}: {details}")
    except Exception as e:  # noqa: BLE001
        print(f"[build] WARNING: failed to log incident: {e}", file=sys.stderr)


def get_con(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ensure_meta(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS _etl_loaded_runs (
            run_id VARCHAR,
            source VARCHAR,
            loaded_at TIMESTAMPTZ,
            rows_loaded INTEGER,
            PRIMARY KEY (run_id, source)
        )
    """)


def file_schema(con: duckdb.DuckDBPyConnection, path: Path) -> list[tuple[str, str]]:
    """Infer (column, type) from a JSONL file, unioning across all records."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_json('{path}', union_by_name=true, "
        f"maximum_object_size=10485760) LIMIT 0").fetchall()
    return [(r[0], r[1]) for r in rows]


def load_bronze_file(con: duckdb.DuckDBPyConnection, source: str, run_id: str,
                     bronze_dir: Path | None = None) -> int:
    """Append one bronze JSONL file into bronze_<source>. Returns rows loaded.

    Idempotent: a run_id already present in _etl_loaded_runs is skipped.
    Schema-evolution safe: new columns in the file are added to the table
    (ALTER TABLE ADD COLUMN) instead of breaking the load; missing columns
    in the file are inserted as NULL. Raw values are never transformed.
    """
    ensure_meta(con)
    table = f"bronze_{source}"
    bdir = bronze_dir or BRONZE_DIR
    already = con.execute(
        "SELECT COUNT(*) FROM _etl_loaded_runs WHERE run_id = ? AND source = ?",
        [run_id, source]).fetchone()[0]
    if already:
        print(f"[build] bronze {source}: run {run_id} already loaded — skipping (idempotent)")
        return 0

    path = bdir / source / f"{run_id}.jsonl"
    if not path.exists():
        print(f"[build] bronze {source}: no file {path} — skipping")
        return 0

    fcols = file_schema(con, path)  # [(name, type)]
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = ?", [table]).fetchone()[0]
    added_columns: list[str] = []
    if not exists:
        cols_ddl = ", ".join(f'"{n}" {t}' for n, t in fcols)
        con.execute(f'CREATE TABLE {table} ({cols_ddl})')
        print(f"[build] bronze {source}: created {table} ({len(fcols)} cols)")
    else:
        tcols = {r[1]: r[2] for r in
                 con.execute(f"PRAGMA table_info('{table}')").fetchall()}
        for name, typ in fcols:
            if name not in tcols:
                con.execute(f'ALTER TABLE {table} ADD COLUMN "{name}" {typ}')
                added_columns.append(name)
                print(f"[build] bronze {source}: schema evolution — "
                      f"added column {name} {typ}")
        if added_columns:
            # Real schema drift on a live feed: warn loudly, keep loading.
            log_incident(run_id, "warning", "schema_drift",
                         f"{source}: new bronze column(s) {added_columns} — "
                         f"staging contract applied, load continued")

    tcols = [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    ftypes = dict(fcols)
    ttypes = {r[1]: r[2] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    # Map each table column to itself when present in the file, else NULL
    # (typed to the table's column type). New file columns were already
    # added to the table above, so they map to themselves.
    select_list = ", ".join(
        f'"{c}"' if c in ftypes else f'CAST(NULL AS {ttypes[c]}) AS "{c}"'
        for c in tcols
    )
    before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.execute(f"""
        INSERT INTO {table} ({", ".join(f'"{c}"' for c in tcols)})
        SELECT {select_list}
        FROM read_json('{path}', union_by_name=true, maximum_object_size=10485760)
    """)
    after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    loaded = after - before
    con.execute("INSERT INTO _etl_loaded_runs VALUES (?, ?, now(), ?)",
                [run_id, source, loaded])
    print(f"[build] bronze {source}: loaded {loaded} rows for run {run_id}")
    return loaded


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="DuckDB file to load bronze into")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--bronze-from", default=None,
                    help="override bronze dir (for drills: warehouse/sandbox)")
    args = ap.parse_args()

    db_path = Path(args.db)
    con = get_con(db_path)
    bdir = Path(args.bronze_from) if args.bronze_from else None
    load_bronze_file(con, "crypto", args.run_id, bronze_dir=bdir)
    load_bronze_file(con, "weather", args.run_id, bronze_dir=bdir)
    con.close()
    print(f"[build] bronze load done -> {db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
