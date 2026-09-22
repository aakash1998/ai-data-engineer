#!/usr/bin/env python3
"""
ingest.py — Bronze ingestion for the AI Data Engineer project.

Pulls REAL data from two free, keyless APIs:
  A. CoinGecko  — top-100 crypto market snapshot (1 call per run)
  B. Open-Meteo — hourly temperature_2m for 20 cities in ONE batched call

Writes immutable bronze JSONL:
  warehouse/bronze/crypto/<run_id>.jsonl
  warehouse/bronze/weather/<run_id>.jsonl

Every record carries _run_id and _ingested_at (UTC).
Bronze files are NEVER mutated after write — raw is immutable.

Retry: exponential backoff on HTTP 429/5xx, max 4 attempts.
If a source is down after retries: exit code 2, incident logged,
NO fabricated data. The freshness test will catch the gap downstream.

Usage:
    python ingest/ingest.py [--run-id RUN_ID] [--source crypto|weather|all]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "warehouse" / "bronze"
RUNS_DIR = PROJECT_ROOT / "runs"

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINGECKO_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "price_change_percentage": "24h",
}

OPENMETEO_URL = "https://api.open-meteo.com/v1/forecast"

# 20 major cities (lat, lon). Calgary first — home turf.
CITIES = [
    ("Calgary", 51.05, -114.07),
    ("Toronto", 43.65, -79.38),
    ("Vancouver", 49.28, -123.12),
    ("New York", 40.71, -74.00),
    ("Los Angeles", 34.05, -118.24),
    ("Mexico City", 19.43, -99.13),
    ("Sao Paulo", -23.55, -46.63),
    ("London", 51.51, -0.13),
    ("Paris", 48.86, 2.35),
    ("Berlin", 52.52, 13.41),
    ("Lagos", 6.52, 3.37),
    ("Cairo", 30.04, 31.24),
    ("Moscow", 55.76, 37.62),
    ("Dubai", 25.20, 55.27),
    ("Mumbai", 19.08, 72.88),
    ("Delhi", 28.61, 77.21),
    ("Singapore", 1.35, 103.82),
    ("Tokyo", 35.68, 139.69),
    ("Sydney", -33.87, 151.21),
    ("Johannesburg", -26.20, 28.04),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_incident(run_id: str, severity: str, check: str, details: str) -> None:
    """Append an incident to the JSONL incident log. Never raises."""
    try:
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        incident = {
            "timestamp": utc_now(),
            "run_id": run_id,
            "severity": severity,  # info | warning | high | critical
            "check": check,
            "details": details,
        }
        with open(RUNS_DIR / "incident_log.jsonl", "a") as f:
            f.write(json.dumps(incident) + "\n")
    except Exception as e:  # noqa: BLE001 — incident logging must not kill the run
        print(f"[ingest] WARNING: failed to log incident: {e}", file=sys.stderr)


def fetch_with_retry(url: str, params: dict, run_id: str, source: str,
                     max_attempts: int = 4) -> requests.Response:
    """GET with exponential backoff on 429/5xx. Raises RuntimeError if all fail."""
    backoff = 2.0
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, params=params, timeout=60,
                                headers={"User-Agent": "ai-data-engineer/1.0"})
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                print(f"[ingest:{source}] attempt {attempt}/{max_attempts}: "
                      f"HTTP {resp.status_code}, backing off {backoff:.0f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        except (requests.RequestException, RuntimeError) as e:
            last_err = e
            if attempt < max_attempts:
                print(f"[ingest:{source}] attempt {attempt}/{max_attempts} failed: {e}; "
                      f"backing off {backoff:.0f}s")
                time.sleep(backoff)
                backoff *= 2
    log_incident(run_id, "critical", "ingest_source_down",
                 f"{source}: all {max_attempts} attempts failed: {last_err}")
    raise RuntimeError(f"{source} unavailable after {max_attempts} attempts: {last_err}")


def ingest_crypto(run_id: str) -> Path:
    """Source A: CoinGecko top-100 markets. 1 call per run."""
    resp = fetch_with_retry(COINGECKO_URL, COINGECKO_PARAMS, run_id, "crypto")
    coins = resp.json()
    if not isinstance(coins, list) or not coins:
        raise RuntimeError("CoinGecko returned empty/unexpected payload")

    ingested_at = utc_now()
    out_dir = BRONZE_DIR / "crypto"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{run_id}.jsonl"

    n = 0
    with open(out_path, "w") as f:
        for c in coins:
            rec = {
                "coin_id": c.get("id"),
                "symbol": c.get("symbol"),
                "name": c.get("name"),
                "current_price": c.get("current_price"),
                "market_cap": c.get("market_cap"),
                "total_volume": c.get("total_volume"),
                "price_change_percentage_24h": c.get("price_change_percentage_24h"),
                "last_updated": c.get("last_updated"),
                "vs_currency": "usd",
                "_run_id": run_id,
                "_ingested_at": ingested_at,
            }
            f.write(json.dumps(rec) + "\n")
            n += 1
    print(f"[ingest:crypto] wrote {n} records -> {out_path}")
    return out_path


def ingest_weather(run_id: str) -> Path:
    """Source B: Open-Meteo hourly temp for 20 cities, ONE batched call."""
    params = {
        "latitude": ",".join(str(c[1]) for c in CITIES),
        "longitude": ",".join(str(c[2]) for c in CITIES),
        "hourly": "temperature_2m",
        "forecast_days": 2,
        "timezone": "UTC",
    }
    resp = fetch_with_retry(OPENMETEO_URL, params, run_id, "weather")
    payload = resp.json()
    if not isinstance(payload, list) or len(payload) != len(CITIES):
        raise RuntimeError(
            f"Open-Meteo returned {len(payload) if isinstance(payload, list) else type(payload)} "
            f"location blocks, expected {len(CITIES)}")

    ingested_at = utc_now()
    out_dir = BRONZE_DIR / "weather"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{run_id}.jsonl"

    n = 0
    with open(out_path, "w") as f:
        for (city, lat, lon), block in zip(CITIES, payload):
            hourly = block.get("hourly") or {}
            times = hourly.get("time") or []
            temps = hourly.get("temperature_2m") or []
            for ts, temp in zip(times, temps):
                rec = {
                    "city": city,
                    "latitude": lat,
                    "longitude": lon,
                    "hour_ts": ts,          # ISO UTC, hourly grain
                    "temperature_2m": temp,  # Celsius
                    "_run_id": run_id,
                    "_ingested_at": ingested_at,
                }
                f.write(json.dumps(rec) + "\n")
                n += 1
    print(f"[ingest:weather] wrote {n} records -> {out_path}")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default=None,
                    help="Run id (default: run-YYYYMMDD-HHMMSS-<8hex>)")
    ap.add_argument("--source", choices=["crypto", "weather", "all"], default="all")
    args = ap.parse_args()

    run_id = args.run_id or ("run-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
                             + "-" + uuid.uuid4().hex[:8])
    print(f"[ingest] run_id={run_id}")

    ok, failed = [], []
    if args.source in ("crypto", "all"):
        try:
            ingest_crypto(run_id); ok.append("crypto")
        except Exception as e:  # noqa: BLE001
            print(f"[ingest:crypto] FAILED: {e}", file=sys.stderr); failed.append("crypto")
    if args.source in ("weather", "all"):
        try:
            ingest_weather(run_id); ok.append("weather")
        except Exception as e:  # noqa: BLE001
            print(f"[ingest:weather] FAILED: {e}", file=sys.stderr); failed.append("weather")

    print(f"[ingest] done. ok={ok} failed={failed}")
    return 0 if not failed else 2


if __name__ == "__main__":
    sys.exit(main())
