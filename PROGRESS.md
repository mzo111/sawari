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

## Constraints discovered

- **AISStream allows one live websocket connection per API key.** A second
  connection (e.g. a probe script) with the same key competes with the running
  worker. Never run a probe while the worker is up; `docker compose stop
  ingest` first. Documented in `.env.example`.
