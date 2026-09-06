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

## 2026-09-06 — port-call detector first run

`ml/portcalls.py` (pure state machine) + `ml/portcalls_job.py` (periodic
asyncpg job) added as the `portcalls` compose service. Ingest container not
touched (`CREATED` still 2026-09-05 22:37:15 -0700). 16 synthetic-track tests;
51 total in the suite.

### Query plan behind the candidate scan (15-minute window, 54,610 positions)

| plan | node | execution |
|---|---|---|
| `ST_DWithin(geography)` only | nested loop, join filter over every row, parallel, cost 3.29M | 357.8 ms |
| geometry bbox `&& ST_Expand(...)` prefilter, then `ST_DWithin(geography)` | **GIST index scan** on `positions_geom_gist`, cost 8.3k, 16,488 candidates → 2,287 pairs | **89.0 ms** |

```
docker compose exec -T db psql -U sawari -d sawari -c "EXPLAIN (ANALYZE, TIMING OFF)
SELECT DISTINCT pos.mmsi, p.id FROM positions pos JOIN ports p
  ON pos.geom && ST_Expand(p.geom, p.anchorage_radius_m / 111000.0 * 1.7)
 AND ST_DWithin(p.geom::geography, pos.geom::geography, p.anchorage_radius_m)
WHERE pos.time > now() - interval '15 minutes';"
```

Anchorage overlap at the default 15 km: exactly one pair, Bremerhaven–
Wilhelmshaven, 29.4 km apart against a 30 km combined reach.

### Backfill and idempotency (`--once --since-minutes 180`, 07:12 UTC)

```
docker compose run --rm -T portcalls python -m ml.portcalls_job --once --since-minutes 180
# run 1: window=180m obs=146199 pairs=2710 opened=2759 arrived=789 departed=155 open_total=2604 stale_open=0 took=0.60s
# run 2: window=180m obs=146274 pairs=2710 opened=0    arrived=0   departed=0   open_total=2604 stale_open=0 took=0.55s
# port_calls count after each: 2759, 2759
```

Second run over the same window opened nothing — replay is a no-op against
the live table. First periodic run of the service (15-minute window):
`obs=16744 pairs=2612 opened=0 arrived=0 departed=1 open_total=2603 stale_open=317 took=0.12s`.
`stale_open` = open calls whose vessel had no fix in the window; never
auto-closed.

### Result, per port (all 2,759 calls)

| port | calls | arrivals | open | closed |
|---|---|---|---|---|
| Amsterdam | 977 | 116 | 921 | 56 |
| Antwerp | 470 | 93 | 446 | 24 |
| Rotterdam | 415 | 116 | 382 | 33 |
| Hamburg | 330 | 187 | 327 | 3 |
| Vlissingen | 176 | 55 | 165 | 11 |
| Bremerhaven | 106 | 62 | 104 | 2 |
| Le Havre | 64 | 41 | 63 | 1 |
| Wilhelmshaven | 64 | 32 | 59 | 5 |
| Southampton | 60 | 33 | 54 | 6 |
| Zeebrugge | 58 | 33 | 48 | 10 |
| Felixstowe | 31 | 19 | 28 | 3 |
| Dunkirk | 8 | 2 | 6 | 2 |

Invariant check (`arrival_at < approach_at OR departure_at < approach_at OR
departure_at < arrival_at`): **0 rows**. Calls per (mmsi, port): 2,664 pairs
with 1, 44 with 2, one each with 3 and 4 — flapping/re-entry is 1.7% of
pairs over three hours.

```
docker compose exec -T db psql -U sawari -d sawari -c "
SELECT p.name, count(*) AS calls, count(*) FILTER (WHERE c.arrival_at IS NOT NULL) AS arrivals,
       count(*) FILTER (WHERE c.departure_at IS NULL) AS open, count(*) FILTER (WHERE c.departure_at IS NOT NULL) AS closed
FROM port_calls c JOIN ports p ON p.id = c.port_id GROUP BY p.name ORDER BY calls DESC;"
```

### Finding: shared MMSIs corrupt departures

Two of five sampled arrivals had physically impossible departures. Tracing
one — MMSI `249600000` at Zeebrugge:

```
            time               |  lon   |   lat   | sog  | km_from_prev | min_from_prev
 2026-09-06 06:05:10 | 3.2128 | 51.3422 |  0.1 |              |
 2026-09-06 06:07:28 | 0.9359 | 51.4836 | 15.5 |        159.2 |  2.3
 2026-09-06 06:11:11 | 3.2128 | 51.3422 |  0.1 |        159.2 |  3.7
```

One transponder is berthed at Zeebrugge, another is underway in the Channel,
both transmitting as `249600000`. The detector followed the interleaved
track exactly as the rules say: departure at 06:07, re-entry at 06:11.

How common: for every closed call, implied speed from the last fix inside
the anchorage to the departure fix —

| closed calls | implied > 60 km/h | implied > 150 km/h | max |
|---|---|---|---|
| 156 | 45 | **44 (28%)** | 7,291,671 km/h |

So roughly a quarter of `departure_at` values in this first run are MMSI
collisions, not vessels leaving, and every dwell time built on them is
wrong. `arrival_at` can be affected the same way (the other ship's fix
lands in the berth circle). Not fixed here — a per-vessel plausibility gate
(max km/h between consecutive fixes) is a rule change, and belongs with the
radii decision. The parser's `IMPOSSIBLE_SPEED` drop only checks reported
`sog`, not distance between fixes, so it does not catch this.

```
docker compose exec -T db psql -U sawari -d sawari -c "
WITH closed AS (
  SELECT c.id, c.mmsi, c.departure_at, p.geom AS pgeom, p.anchorage_radius_m AS r
  FROM port_calls c JOIN ports p ON p.id = c.port_id WHERE c.departure_at IS NOT NULL),
last_inside AS (
  SELECT cl.id, max(pos.time) AS t_in FROM closed cl
  JOIN positions pos ON pos.mmsi = cl.mmsi AND pos.time < cl.departure_at
   AND ST_DWithin(cl.pgeom::geography, pos.geom::geography, cl.r) GROUP BY cl.id),
jump AS (
  SELECT ST_Distance(pi.geom::geography, po.geom::geography)/1000.0 AS km,
         EXTRACT(EPOCH FROM (cl.departure_at - li.t_in))/3600.0 AS hours
  FROM closed cl JOIN last_inside li ON li.id = cl.id
  JOIN positions pi ON pi.mmsi = cl.mmsi AND pi.time = li.t_in
  JOIN positions po ON po.mmsi = cl.mmsi AND po.time = cl.departure_at)
SELECT count(*), count(*) FILTER (WHERE km/NULLIF(hours,0) > 60),
       count(*) FILTER (WHERE km/NULLIF(hours,0) > 150), round(max(km/NULLIF(hours,0))::numeric) FROM jump;"
```

## 2026-09-06 — plausibility gate on port-call observations

`ml/portcalls_job.py` now computes implied speed between each fix and its
neighbours (`lag`/`lead` per MMSI, in the observation query) and
`ml/portcalls.plausible()` drops any fix whose jump to **either** neighbour
exceeds `MAX_KMH = 60`. `positions` is untouched; the parser and worker are
untouched; this filters at label time only. 9 new tests, 60 total.

Both-sides semantics, deliberately: under a shared MMSI every fix has an
implausible neighbour, so the whole track contributes no labels. A running
filter against the last accepted fix would keep whichever transponder
reported first — a confident label for a vessel we can't identify.

### Before / after, same 180-minute window, `port_calls` truncated between runs

Before = the previous image; after = rebuilt with the gate. 68 s apart.

```
docker compose stop portcalls
docker compose exec -T db psql -U sawari -d sawari -c "TRUNCATE port_calls;"
docker compose run --rm -T portcalls python -m ml.portcalls_job --once --since-minutes 180
# before (07:21:07): window=180m obs=157485 pairs=2728 opened=2779 arrived=796 departed=164 took=0.65s
docker compose exec -T db psql -U sawari -d sawari -c "TRUNCATE port_calls;"
docker compose build -q portcalls
docker compose run --rm -T portcalls python -m ml.portcalls_job --once --since-minutes 180
# after  (07:22:15): window=180m obs=156601 gated=2257 pairs=2730 opened=2736 arrived=786 departed=122 took=1.27s
```

| | calls | arrivals | closed | implied >60 km/h | implied >150 km/h | max km/h |
|---|---|---|---|---|---|---|
| before | 2,779 | 796 | 164 | 46 | **45 (27.4%)** | 7,291,671 |
| after | 2,736 | 786 | 122 | 1 | **0** | 60 |

Gated: 2,257 of 158,858 fetched observations (1.4%). Calls −1.5%,
arrivals −1.3%, closed −25.6% — the removed closures are the collisions.
Invariant check after: 0 rows. The single residual >60 sits at the
threshold: the check query (`PROGRESS.md` shared-MMSI section) walks raw
`positions` from the last in-anchorage fix to the departure fix, so a path
that crosses a gated fix can show a straight-line speed at the boundary.

Per port, after (the per-port table in the detector section above is from
the 07:12 run, so it is not a like-for-like "before"):

| port | calls | arrivals | closed |
|---|---|---|---|
| Amsterdam | 948 | 113 | 21 |
| Antwerp | 476 | 93 | 28 |
| Rotterdam | 415 | 118 | 30 |
| Hamburg | 331 | 187 | 3 |
| Vlissingen | 178 | 56 | 13 |
| Bremerhaven | 105 | 61 | 1 |
| Le Havre | 64 | 41 | 1 |
| Wilhelmshaven | 64 | 32 | 8 |
| Southampton | 60 | 34 | 6 |
| Zeebrugge | 57 | 31 | 7 |
| Felixstowe | 30 | 18 | 2 |
| Dunkirk | 8 | 2 | 2 |

First periodic run after restart (15-minute window):
`obs=16656 gated=170 pairs=2627 opened=0 arrived=0 departed=0 open_total=2614 stale_open=332 took=0.16s`.
`stale_open` rose from 317 to 332: an open call whose only fixes in the
window were gated now counts as stale.

Cost of the gate, measured on a 15-minute window before building it: the
`lag`/`lead` window pass is 128 ms over 54,510 rows (single WindowAgg,
5 MB quicksort); it removes 372 fixes (0.68%) touching 119 MMSIs (1.5%),
2 of them entirely. Threshold consequence: 60 km/h is 32 kn, so
high-speed craft above that are excluded from labels; the parser's sog
ceiling is 50 kn.

## 2026-09-06 — naive ETA baseline, temporal holdout

`ml/baseline.py`: `eta_hours = (dist_m / 1852) / max(sog, 1 kn)` — no
training. Distance is PostGIS `ST_Distance(geography)` to the port point.
`sog` below 1 kn (or `NULL`) is clamped to 1 kn: below that the reported
speed is GPS jitter on a ship not making way, so the floor gives a finite,
pessimistic prediction instead of dividing by noise. `ml/eval.py` builds
the dataset (in-anchorage fixes strictly before `arrival_at`, on calls
with an observed approach phase, gated by the same `MAX_KMH = 60` rule the
detector uses), splits **by call on `arrival_at`** — never by row — and
reports MAE. 14 tests; 74 in the suite.

```
.venv/bin/python -m ml.eval
```

```
dataset: rows=3165 calls=74 gate=MAX_KMH 60 floor=1 kn holdout_frac=0.2
temporal cutoff on arrival_at: 2026-09-06T06:59:14.575731+00:00
```

Of 788 calls with an `arrival_at`, only 74 were observed approaching
(`approach_at < arrival_at`); the rest were already berthed at first
sight. Labels are capped by the data span (max ≈ 2 h to arrival).

### Holdout (`arrival_at >= cutoff`): 1,242 rows, 15 calls

| | rows | MAE |
|---|---|---|
| all rows | 1,242 | **0.874 h (52.5 min)** |
| `sog >= 1 kn` only | 963 | 0.443 h (26.6 min) |
| floored rows (`sog < 1 kn` or NULL) | 279 (22.5%) | — |

| distance band | n | MAE |
|---|---|---|
| 0–5 km | 797 | 0.575 h (34.5 min) |
| 5–10 km | 281 | 0.864 h (51.9 min) |
| 10–15 km | 164 | 2.344 h (140.7 min) |

| port | rows | calls | MAE |
|---|---|---|---|
| Rotterdam | 431 | 6 | 0.891 h (53.5 min) |
| Amsterdam | 248 | 2 | 1.573 h (94.4 min) |
| Southampton | 237 | 3 | 0.493 h (29.6 min) |
| Zeebrugge | 104 | 1 | 0.872 h (52.3 min) |
| Antwerp | 100 | 1 | 0.751 h (45.0 min) |
| Hamburg | 69 | 1 | 0.367 h (22.0 min) |
| Vlissingen | 53 | 1 | 0.074 h (4.5 min) |

### Train side, reference only (`arrival_at < cutoff`): 1,923 rows, 59 calls

The baseline is not fitted, so this is the same predictor on more data —
it shows whether the holdout is representative, nothing else.

| | rows | MAE |
|---|---|---|
| all rows | 1,923 | 0.601 h (36.1 min) |
| `sog >= 1 kn` only | 1,593 | 0.309 h (18.6 min) |
| floored rows | 330 (17.2%) | — |

| distance band | n | MAE |
|---|---|---|
| 0–5 km | 1,334 | 0.410 h (24.6 min) |
| 5–10 km | 464 | 1.080 h (64.8 min) |
| 10–15 km | 125 | 0.862 h (51.7 min) |

| port | rows | calls | MAE |
|---|---|---|---|
| Rotterdam | 389 | 8 | 0.849 h (51.0 min) |
| Hamburg | 326 | 11 | 0.369 h (22.1 min) |
| Amsterdam | 315 | 6 | 0.478 h (28.7 min) |
| Le Havre | 269 | 6 | 0.529 h (31.8 min) |
| Antwerp | 234 | 7 | 0.972 h (58.3 min) |
| Vlissingen | 129 | 5 | 0.837 h (50.2 min) |
| Southampton | 92 | 6 | 0.497 h (29.8 min) |
| Wilhelmshaven | 90 | 3 | 0.113 h (6.8 min) |
| Bremerhaven | 54 | 3 | 0.316 h (19.0 min) |
| Zeebrugge | 15 | 1 | 0.125 h (7.5 min) |
| Felixstowe | 10 | 3 | 0.180 h (10.8 min) |

Per-port holdout figures rest on 1–6 calls each. The number to beat is
the holdout all-rows MAE, **0.874 h**, on this cutoff; re-run `ml.eval`
when the model is evaluated so both use the same dataset and split.

## 2026-09-06 — XGBoost ETA model, first run (pipeline validation only)

**64 training calls is too small for this result to mean anything.** The
run exists to prove the pipeline — dataset → features → train → holdout →
registry — before the dataset grows. Read the numbers as "it works," not
as "it's good."

`ml/dataset.py` is now the single definition of the labelled dataset
(shared by `ml.eval` and `ml.train`); `ml/features.py` is pure feature
engineering; `ml/train.py` trains with XGBoost's native `xgb.train` at its
documented defaults (`reg:squarederror`, `eta 0.3`, `max_depth 6`, 100
rounds, `seed 0`) and scores baseline and model on the **identical**
temporal split-by-call. Native API because `XGBRegressor` in XGBoost 3.x
requires scikit-learn, which is not in the stack. Pin: `xgboost-cpu==3.4.1`
in `requirements-ml.txt` (23 MB, numpy + scipy only; the GPU `xgboost`
wheel drags `nvidia-nccl-cu13`). 12 new tests; 86 in the suite.

**Missing vessel attributes → NaN, handled natively by XGBoost** (a default
branch per split); no imputation. Measured NaN share in train: `draught_m`
24.9%, `cog` / `bearing_minus_cog` 5.3%, everything else 0%.

```
.venv/bin/python -m ml.train
```

```
dataset: rows=3513 calls=81 gate=MAX_KMH 60 baseline_floor=1 kn holdout_frac=0.2 xgboost=3.4.1
temporal cutoff on arrival_at: 2026-09-06T07:04:40.190297+00:00
train:   rows=2415 calls=64
holdout: rows=1098 calls=17
```

The dataset grew since the baseline entry (3,165 → 3,513 rows; cutoff
06:59:14 → 07:04:40), so the baseline is **recomputed on this split** —
that is the like-for-like number, not the 0.874 h recorded earlier.

### Holdout — 1,098 rows, 17 calls (train: 64 calls)

| predictor | MAE, all rows | MAE, `sog >= 1 kn` (913 rows) |
|---|---|---|
| baseline (dist / speed, 1 kn floor) | **0.814 h (48.9 min)** | 0.399 h |
| XGBoost, defaults | **0.394 h (23.6 min)** | 0.360 h |
| improvement | **+51.7%** | +9.8% |

| distance band | n | baseline | xgboost |
|---|---|---|---|
| 0–5 km | 575 | 0.440 h | 0.339 h |
| 5–10 km | 296 | 0.715 h | 0.429 h |
| 10–15 km | 227 | 1.892 h | 0.486 h |

| port | rows | calls | baseline | xgboost |
|---|---|---|---|---|
| Rotterdam | 565 | 10 | 0.703 h | 0.358 h |
| Southampton | 143 | 2 | 0.425 h | 0.214 h |
| Amsterdam | 117 | 1 | 2.556 h | 0.325 h |
| Zeebrugge | 104 | 1 | 0.872 h | 0.807 h |
| Wilhelmshaven | 80 | 1 | 0.179 h | 0.512 h |
| Vlissingen | 53 | 1 | 0.074 h | 0.211 h |
| Antwerp | 36 | 1 | 0.787 h | 0.704 h |

Seven of the eleven ports in the holdout have exactly one call.

### In-sample, reference only — 2,415 rows, 64 calls

XGBoost train MAE **0.008 h (0.5 min)** across every band and port. The
model reproduces its 64 training calls almost exactly; that is
memorisation, and it is why the holdout number is the only one that
counts.

### Feature importances (gain share, split count)

| feature | gain | splits |
|---|---|---|
| length_m | 23.5% | 187 |
| width_m | 16.0% | 158 |
| dist_m | 12.8% | 1,057 |
| draught_m | 12.4% | 220 |
| bearing_minus_cog | 9.7% | 563 |
| hour_utc | 9.3% | 695 |
| sog | 6.8% | 617 |
| ship_type | 6.3% | 116 |
| cog | 2.0% | 609 |
| minutes_in_anchorage | 1.4% | 385 |

With 64 calls, `length_m` and `width_m` are effectively vessel
identifiers: the highest-gain features let the trees find *which ship*,
not how ships approach. Expect this ranking to change as calls accumulate
across many vessels; `dist_m` carrying the most splits but not the most
gain is the tell.

### Registry

```
docker compose exec -T db psql -U sawari -d sawari -x -c \
  "SELECT * FROM model_registry ORDER BY id DESC LIMIT 1;"
# id=1 name=xgb_eta version=20260906T074941Z train_rows=2415
# holdout_start=2026-09-06 07:04:40 baseline_mae_hours=0.8145 model_mae_hours=0.3936
# artifact_path=ml/artifacts/xgb_eta_20260906T074941Z.json is_active=f
# params: features, xgb_params, cutoff, train_calls=64, holdout_calls=17, nan_share_train, improvement_pct=51.7
```

Artifact is 530 KB, gitignored (`ml/artifacts/`). `is_active` stays
false — nothing serves this model.

## 2026-09-06 — vessel-leakage diagnostics (2×2, one snapshot, no registry writes)

`ml.train --diagnostics` loads the dataset once and runs split ∈
{temporal, group} × features ∈ {all 10, without `ship_type`/`length_m`/
`width_m`/`draught_m`}. `group` = the temporal split with every vessel
that appears in the holdout purged from training; the holdout is
byte-for-byte the temporal holdout. Default split unchanged (`temporal`).
XGBoost defaults unchanged. 4 new tests; 90 in the suite.

```
.venv/bin/python -m ml.train --diagnostics
```

### Overlap under the temporal split (cutoff 2026-09-06T07:18:26Z)

| | |
|---|---|
| dataset | 3,953 rows, 86 calls, **85 distinct MMSIs** |
| calls per vessel | 84 vessels with one call, 1 with two |
| train | 68 calls, 68 vessels, 2,769 rows |
| holdout | 18 calls, 18 vessels, 1,184 rows |
| MMSIs on both sides | **1** |
| holdout calls whose vessel is in train | **1 of 18** |
| training after purge (group split) | 67 calls, 67 vessels, 2,766 rows |

### Holdout MAE, all four cells (holdout identical in every cell: 1,184 rows, 18 calls)

| split | features | train calls / vessels / rows | baseline | model | improvement |
|---|---|---|---|---|---|
| temporal | all (10) | 68 / 68 / 2,769 | 0.918 h | 0.480 h | +47.7% |
| temporal | no vessel attrs (6) | 68 / 68 / 2,769 | 0.918 h | **0.471 h** | +48.7% |
| group | all (10) | 67 / 67 / 2,766 | 0.918 h | 0.488 h | +46.8% |
| group | no vessel attrs (6) | 67 / 67 / 2,766 | 0.918 h | **0.469 h** | +48.9% |

In-sample (train) model MAE: 0.011 h with all features, 0.020 h without
the vessel attributes; 0.011 h / 0.021 h under the group split.

Holdout by distance band, temporal split:

| band | n | baseline | all (10) | no vessel attrs (6) |
|---|---|---|---|---|
| 0–5 km | 567 | 0.462 h | 0.355 h | 0.335 h |
| 5–10 km | 348 | 0.861 h | 0.563 h | 0.581 h |
| 10–15 km | 269 | 1.953 h | 0.635 h | 0.615 h |

Holdout by port, temporal split (calls in parentheses):

| port | rows | baseline | all (10) | no vessel attrs (6) |
|---|---|---|---|---|
| Rotterdam (9) | 547 | 0.666 h | 0.442 h | 0.457 h |
| Amsterdam (2) | 191 | 1.896 h | 0.387 h | 0.375 h |
| Felixstowe (1) | 131 | 1.684 h | 0.748 h | 0.734 h |
| Zeebrugge (1) | 104 | 0.872 h | 0.785 h | 0.748 h |
| Wilhelmshaven (1) | 80 | 0.179 h | 0.479 h | 0.388 h |
| Vlissingen (1) | 53 | 0.074 h | 0.231 h | 0.149 h |
| Southampton (2) | 42 | 0.067 h | 0.071 h | 0.117 h |
| Antwerp (1) | 36 | 0.787 h | 0.532 h | 0.508 h |

### Feature importances (gain share, split count)

| feature | temporal, all | temporal, no vessel attrs | group, all | group, no vessel attrs |
|---|---|---|---|---|
| length_m | 20.5% (220) | — | 21.8% (192) | — |
| width_m | 18.2% (153) | — | 19.1% (154) | — |
| draught_m | 14.8% (230) | — | 13.3% (245) | — |
| dist_m | 12.9% (1,002) | **31.4%** (1,267) | 12.0% (1,053) | **29.4%** (1,235) |
| ship_type | 8.1% (114) | — | 8.7% (107) | — |
| hour_utc | 8.1% (717) | 24.2% (827) | 8.3% (697) | 23.5% (815) |
| sog | 7.1% (669) | 18.2% (780) | 7.4% (629) | 18.0% (825) |
| bearing_minus_cog | 6.9% (577) | 12.7% (718) | 6.2% (602) | 13.2% (687) |
| minutes_in_anchorage | 2.0% (385) | 5.6% (439) | 2.1% (369) | 6.5% (425) |
| cog | 1.4% (576) | 8.0% (814) | 1.2% (529) | 9.4% (771) |

Dataset time span for reference against `hour_utc`: `arrival_at` from
05:12 to 07:35 UTC on one day.

Nothing was written to `model_registry` (still 1 row) or `ml/artifacts/`.

## Constraints discovered

- **AISStream allows one live websocket connection per API key.** A second
  connection (e.g. a probe script) with the same key competes with the running
  worker. Never run a probe while the worker is up; `docker compose stop
  ingest` first. Documented in `.env.example`.
