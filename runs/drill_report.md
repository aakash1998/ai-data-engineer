# Chaos Drill Report — 2026-09-22T15:41:10.691970+00:00

Drills run against **sandbox copies** of real bronze batch `run-20260922-154029-102c87ce`
(100 crypto records, 960 weather records).
Real bronze/silver/gold were never touched. Drill run_ids are prefixed `drill-`.

## Fault 1 — Duplicate delivery (simulates a retried API request)

- Sandbox file contained **200** records (batch delivered twice in one payload).
- Loader 1st attempt: **200** rows. Loader 2nd attempt (same run_id): **0** rows — idempotency held.
- Silver crypto rows after dedupe: **100** (clean count, no dupes).
- Uniqueness test on `(coin_id, snapshot_ts)`: **pass**.
- Verdict: PASS — the retried request changed nothing downstream.

## Fault 2 — Null flood on `total_volume` (simulates an exchange feed glitch)

- **30** of 100 records had `total_volume` nulled (30%).
- `not_null:silver_crypto.total_volume`: **fail** (expected failure).
- Quarantined: **30** rows → `quarantine_crypto` with reason + run_id.
- Pipeline completed: silver **70** rows, gold daily mart **70** rows (built on clean data only).
- Tests: 11 passed, 1 failed, 0 warned. Incident logged at severity high.
- Verdict: PASS — bad rows isolated, pipeline stayed green, on-call got paged (incident log).

## Fault 3 — Schema drift (simulates an API change mid-day)

- **40** records renamed `total_volume` → `volume_24h`; **20** gained a new field `fdv_v2`.
- Bronze loader absorbed both new columns via schema evolution (no failure).
- Silver staging `COALESCE(total_volume, volume_24h)`: **100** of 100 silver rows carry a volume.
- `not_null:silver_crypto.total_volume`: **pass**.
- Tests: 12 passed, 0 failed, 0 warned. Warning incident `schema_drift` logged.
- Verdict: PASS — no hard failure; the contract absorbed the rename.

## Animation beats (for the LinkedIn video)

1. Bronze → silver → gold flow with live row counts.
2. Fault 1: the same truck arriving twice; the gate (idempotency) waves the second one through with "already seen".
3. Fault 2: red flash on the volume column; bad rows diverted to a quarantine cage; pipeline keeps flowing green.
4. Fault 3: a column morphs its name mid-stream; a COALESCE bridge carries the values across; warning badge pops.
