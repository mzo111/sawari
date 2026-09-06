# PROGRESS

Measured values only. Every number has the command that produced it.
Times are UTC from the ingest container clock / `positions.time`.

## 2026-09-06 — first live ingest, worldwide box

Worker rebuilt with `AIS_BOUNDING_BOXES=[[[-90,-180],[90,180]]]` and
`ping_timeout=60` (commit `40e2db0`). Ran locally on WSL2 against the
compose stack; **not** the VPS.

### Message rate

| metric | value | source |
|---|---|---|
| positions written, whole table | 83,767 rows / 18,536 distinct MMSI | query A |
| written rate incl. reconnect gaps | 7,829 rows/min (~130 rows/s) | query A, over 05:11:59 → 05:22:41 |
| received rate, first container | 8,996 msg/min (~150 msg/s) | heartbeat: `received=44978` at 05:16:59, 5 min after subscribe at 05:12:00 |

`received > written` is expected, not loss: `ShipStaticData` messages count
as received but go to the `vessels` upsert, not `positions`; and up to 500
rows sit in the batch buffer at any heartbeat instant.

Query A:

```
docker compose exec -T db psql -U sawari -d sawari -c "
SELECT count(*) AS total_rows, count(DISTINCT mmsi) AS vessels,
       min(time), max(time),
       round(count(*) / (EXTRACT(EPOCH FROM (max(time)-min(time)))/60.0)) AS rows_per_min_incl_gaps
FROM positions;"
```

Heartbeat source: `docker compose logs ingest | grep heartbeat`. Note the
in-memory counters reset to zero on every container restart, so rates from
heartbeats must be read as deltas between consecutive lines within one
container lifetime.

### Region breakdown (worldwide box, first ~10.7 min)

| region | positions | % |
|---|---|---|
| N Europe | 50,395 | 60.52 |
| other | 12,182 | 14.63 |
| Mediterranean | 10,519 | 12.63 |
| US coasts | 10,171 | 12.21 |
| **Arabian Gulf** | **0** | **0.00** |
| **Gulf of Oman** | **0** | **0.00** |

Bucket bounds (`ST_X` = lon, `ST_Y` = lat), evaluated in this order:

```
docker compose exec -T db psql -U sawari -d sawari -c "
WITH tagged AS (
  SELECT CASE
    WHEN ST_Y(geom) BETWEEN 22 AND 30.5 AND ST_X(geom) BETWEEN 47   AND 56.5 THEN 'Arabian Gulf'
    WHEN ST_Y(geom) BETWEEN 22 AND 27   AND ST_X(geom) BETWEEN 56.5 AND 60.5 THEN 'Gulf of Oman'
    WHEN ST_Y(geom) BETWEEN 48 AND 72   AND ST_X(geom) BETWEEN -12  AND 32   THEN 'N Europe'
    WHEN ST_Y(geom) BETWEEN 30 AND 46   AND ST_X(geom) BETWEEN -6   AND 37   THEN 'Mediterranean'
    WHEN ST_Y(geom) BETWEEN 24 AND 49   AND ST_X(geom) BETWEEN -125 AND -65  THEN 'US coasts'
    ELSE 'other' END AS region
  FROM positions
)
SELECT region, count(*) AS positions,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM tagged GROUP BY region ORDER BY positions DESC;"
```

Gulf zero confirmed with a deliberately wide box (whole Gulf, Gulf of Oman,
Arabian Sea, Red Sea approaches) to rule out bucket-bound error:

```
docker compose exec -T db psql -U sawari -d sawari -tAc \
  "SELECT count(*) FROM positions WHERE ST_X(geom) BETWEEN 44 AND 62 AND ST_Y(geom) BETWEEN 12 AND 32;"
# -> 0
```

Consistent with the earlier one-shot probe (`ops/probe*.py`) where the Gulf
box returned nothing in 25 s while the worldwide box returned ~170 msg/s.

**Conclusion: the original "connects but writes zero rows" symptom was not a
bug in the worker. The write path was correct; AISStream delivered no
messages inside the Gulf bounding boxes.**

### Reconnects observed

Two stream drops in ~10 min, on two different signatures, both self-healed
via the 1 s backoff and confirmed by `SubscriptionConfirmation` incrementing:

| time | error | outcome |
|---|---|---|
| 05:14:07 | `keepalive ping timeout; no close frame received` (1011) | resubscribed; `ping_timeout` raised 20 → 60 in response |
| 05:20:31 | `no close frame received or sent` | resubscribed at 05:20:32 — TCP-level close, not a ping timeout; unexplained |

Source: `docker compose logs ingest | grep -E "WARNING|subscribed"`.

## 2026-09-06 — region pivot to southern North Sea / Channel

Decision: keep AISStream and the architecture, re-scope the target region to
one the feed actually covers. Default box is now a single
`[[49.0, -2.5], [56.0, 10.0]]` (49–56°N, 2.5°W–10°E). Gulf ports replaced by
12 North Sea / Channel ports in `db/schema.sql` and the live DB
(`port_calls` was empty, so no FK impact). Worldwide rows from the earlier
run were kept, not deleted.

### Why this box — density in the worldwide sample

Top 2° cells in the N Europe bucket by distinct vessels, from the 83,767-row
worldwide run above:

| lat_cell | lon_cell | positions | vessels |
|---|---|---|---|
| 50 | 4 | 12,630 | 2,512 |
| 52 | 4 | 14,701 | 2,127 |
| 52 | 8 | 4,142 | 625 |
| 50 | 2 | 3,139 | 594 |
| 52 | 6 | 2,602 | 485 |
| 54 | 10 | 1,428 | 251 |
| 56 | 10 | 1,650 | 231 |
| 50 | 6 | 1,029 | 227 |
| 50 | 0 | 1,289 | 216 |
| 54 | 18 | 949 | 192 |
| 58 | 10 | 1,066 | 188 |
| 50 | -2 | 1,148 | 177 |
| 54 | 12 | 981 | 157 |
| 58 | 18 | 1,029 | 141 |
| 58 | 4 | 712 | 139 |

Nine of the top fifteen cells fall inside the chosen box; the Baltic and
Norwegian cells were left out deliberately (focus over breadth).

```
docker compose exec -T db psql -U sawari -d sawari -c "
SELECT floor(ST_Y(geom)/2)*2 AS lat_cell, floor(ST_X(geom)/2)*2 AS lon_cell,
       count(*) AS positions, count(DISTINCT mmsi) AS vessels
FROM positions
WHERE ST_Y(geom) BETWEEN 48 AND 72 AND ST_X(geom) BETWEEN -12 AND 32
GROUP BY 1,2 ORDER BY vessels DESC LIMIT 15;"
```

### First run on the new box

Worker rebuilt and resubscribed at 05:29:15 with no `AIS_BOUNDING_BOXES`
override (i.e. the code default).

| metric | value | source |
|---|---|---|
| first heartbeat | `received=4133 written=3409 rate=3408/min reconnects=0 drops[bad_mmsi=3, unhandled_SubscriptionConfirmation=1]` | `docker compose logs ingest \| grep heartbeat`, 05:30:14 |
| second heartbeat | `received=8482 written=7075 rate=3536/min reconnects=0 drops[bad_mmsi=5, unhandled_SubscriptionConfirmation=1]` | same, 05:31:15 |
| rows inside box / total since restart | **4,333 / 4,333** | query B, 05:29:15 → 05:30:28 |
| distinct vessels in first 73 s | 3,563 | query B |

Every row written since the restart is inside the box — AISStream filters
server-side, so anything outside would have been a finding. Rate is roughly
half the worldwide figure, consistent with the box being a subset.

Query B:

```
docker compose exec -T db psql -U sawari -d sawari -c "
SELECT count(*) FILTER (WHERE ST_Y(geom) BETWEEN 49 AND 56 AND ST_X(geom) BETWEEN -2.5 AND 10) AS in_box,
       count(*) AS total, count(DISTINCT mmsi) AS vessels, min(time), max(time)
FROM positions WHERE time > '2026-09-06 05:29:15+00';"
```

### Open item

Port coordinates in `db/schema.sql` are approximate (~0.05°) and unverified
against UN/LOCODE. `berth_radius_m` is 3 km, so a wrong basin is possible.
Verify each before trusting `port_calls`. Does not affect ingest.

## 2026-09-06 — API service first run

`api/` (FastAPI + asyncpg pool + Redis subscriber) added as the `api` compose
service on :8000. Ingest container not rebuilt or restarted (its `CREATED`
stayed 2026-09-05 22:37:15 -0700 throughout). New pins in `requirements.txt`:
`fastapi==0.141.1`, `uvicorn==0.52.4`; `websockets` stays `13.1`.

### Request latency (single requests, local Docker, `curl -w %{time_total}`)

| request | HTTP | time |
|---|---|---|
| `GET /health` | 200 | 0.008 s |
| `GET /vessels?minutes=10&limit=100` | 200 | 0.019 s |
| `GET /vessels?minutes=10&limit=100&offset=7400` (last page of ~7,500 active) | 200 | 0.021 s |
| `GET /vessels/200000000/track?hours=24` | 200 | 0.002 s |
| `GET /vessels/999/track` (mmsi out of range) | 422 | 0.001 s |
| `GET /vessels?minutes=99999` | 422 | 0.001 s |
| `GET /docs`, `GET /openapi.json` | 200 | < 0.003 s |

```
B=http://localhost:8000
for p in /health "/vessels?minutes=10&limit=100" "/vessels?minutes=10&limit=100&offset=7400"; do
  curl -s -o /dev/null -w "$p -> %{http_code} %{time_total}s\n" "$B$p"; done
```

`/health` at 06:48:16 reported `rows_approx=411799` (from
`approximate_row_count('positions')`, stats-based) and
`stream.dropped=0`.

Planner evidence behind `/vessels` (`EXPLAIN ANALYZE`, live data): Timescale
**SkipScan** over the `(mmsi, time DESC)` unique index; 23.3 ms execution at
`OFFSET 7400`. `/health`'s `max(time)` is an index-only scan on the
auto-created `_hyper_1_1_chunk_positions_time_idx`, 0.059 ms.

### WebSocket bridge (`/ws/positions`)

Smoke client: connect, assert the first 5 frames carry
`mmsi, lat, lon, sog, cog, time`, then count frames for 5 s, then close.

| server | frames/s over 5 s | close |
|---|---|---|
| container, uvicorn default (`--ws auto` → sansio impl) — **final config** | 58.7 | clean, `INFO` only |
| container, `--ws websockets` (legacy impl, since removed) | 66.0 | clean |
| local `.venv` uvicorn :8001, `--ws websockets-sansio` | 61.7 | clean |

All three are the ingest rate (3,882 rows/min ≈ 64.7/s) within the 5 s
sampling window. `kill -9` of a client mid-stream: no traceback, `clients`
back to 0, `/health` still 200.

```
.venv/bin/python <scratchpad>/ws_smoke.py ws://localhost:8000/ws/positions
docker compose logs api | grep -E "ws client|Traceback"
```

**Compatibility finding:** uvicorn 0.52.4's default WebSocket implementation
(`websockets-sansio`) works with `websockets==13.1` — verified by the
:8001 run above, then by the rebuilt container. The plan had pinned
`--ws websockets` on the assumption it might not; uvicorn logs a
deprecation warning for that flag, so it was dropped once measured.

Data-quality aside: the first vessel in `/vessels` is MMSI `200000000`
(name `null`), which is in range but almost certainly a misconfigured
transponder. Not dropped by the parser — worth a `DropReason` later if it
skews the model.

## Constraints discovered

- **AISStream allows one live websocket connection per API key.** A second
  connection (e.g. a probe script) with the same key competes with the running
  worker. Never run a probe while the worker is up; `docker compose stop
  ingest` first. Documented in `.env.example`.
