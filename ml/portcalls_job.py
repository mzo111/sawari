"""Periodic port-call detector.

  python -m ml.portcalls_job [--since-minutes 15] [--interval 60] [--once]

Each run reads recent positions, measures them against ports in PostGIS,
hands the result to the pure state machine in ml.portcalls, and writes
port_calls. Runs overlap on purpose; the state machine makes replay a no-op.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import time
from datetime import datetime, timedelta, timezone

import asyncpg

from .portcalls import Call, Obs, Port, detect, plausible

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("portcalls")

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sawari:sawari@localhost:5432/sawari"
)

# Implied speed between consecutive fixes above this is a position jump, not
# movement: in the first run 28% of closed calls had departures implying
# >150 km/h from two transponders sharing one MMSI (PROGRESS.md 2026-09-06).
# 60 km/h is 32 kn, stricter than the parser's 50 kn sog ceiling.
MAX_KMH = 60.0

PORTS = "SELECT id, anchorage_radius_m, berth_radius_m FROM ports"

RELEVANT_CALLS = """
SELECT id, mmsi, port_id, approach_at, arrival_at, departure_at
FROM port_calls
WHERE departure_at IS NULL OR departure_at > $1
"""

# The geometry bbox test drives the GIST index; the geography check after it
# is the one that decides. 1.7 covers longitude shrink up to 56N (1/cos 56 = 1.8
# would be exact; the box only has to be a superset). Measured 89 ms vs 358 ms
# for the plain geography join over a 15-minute window.
CANDIDATE_PAIRS = """
SELECT DISTINCT pos.mmsi, p.id AS port_id
FROM positions pos
JOIN ports p
  ON pos.geom && ST_Expand(p.geom, p.anchorage_radius_m / 111000.0 * 1.7)
 AND ST_DWithin(p.geom::geography, pos.geom::geography, p.anchorage_radius_m)
WHERE pos.time > $1
"""

# Every position of the vessel in the window, not just in-range ones:
# departure is the first fix *outside* the anchorage. Implied speed to the
# neighbouring fixes is measured here, once per MMSI, and gated in Python.
OBSERVATIONS = """
WITH fixes AS (
    SELECT mmsi, time, geom, sog,
           ST_Distance(geom::geography, (lag(geom) OVER w)::geography)
             / NULLIF(EXTRACT(EPOCH FROM (time - lag(time) OVER w)), 0) * 3.6 AS kmh_prev,
           ST_Distance(geom::geography, (lead(geom) OVER w)::geography)
             / NULLIF(EXTRACT(EPOCH FROM (lead(time) OVER w - time)), 0) * 3.6 AS kmh_next
    FROM positions
    WHERE mmsi = ANY($1::bigint[]) AND time > $3
    WINDOW w AS (PARTITION BY mmsi ORDER BY time)
)
SELECT f.mmsi, k.port_id, f.time,
       ST_Distance(p.geom::geography, f.geom::geography) AS dist_m,
       f.sog, f.kmh_prev, f.kmh_next
FROM fixes f
JOIN unnest($1::bigint[], $2::int[]) AS k(mmsi, port_id) ON k.mmsi = f.mmsi
JOIN ports p ON p.id = k.port_id
ORDER BY f.mmsi, k.port_id, f.time
"""

INSERT_CALL = """
INSERT INTO port_calls (mmsi, port_id, approach_at, arrival_at, departure_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (mmsi, port_id, approach_at) DO NOTHING
"""

UPDATE_CALL = """
UPDATE port_calls
SET arrival_at   = COALESCE(arrival_at, $2),
    departure_at = COALESCE(departure_at, $3)
WHERE id = $1
"""

COUNT_OPEN = "SELECT count(*) FROM port_calls WHERE departure_at IS NULL"


async def run_once(pool: asyncpg.Pool, since_minutes: int) -> dict[str, float | int]:
    started = time.monotonic()
    since = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)

    async with pool.acquire() as conn, conn.transaction():
        ports = {
            r["id"]: Port(r["id"], r["anchorage_radius_m"], r["berth_radius_m"])
            for r in await conn.fetch(PORTS)
        }
        calls = [
            Call(r["mmsi"], r["port_id"], r["approach_at"],
                 r["arrival_at"], r["departure_at"], r["id"])
            for r in await conn.fetch(RELEVANT_CALLS, since)
        ]
        open_before = [c for c in calls if c.is_open]
        before = {id(c): (c.arrival_at, c.departure_at) for c in calls}

        pairs = {(r["mmsi"], r["port_id"]) for r in await conn.fetch(CANDIDATE_PAIRS, since)}
        pairs |= {(c.mmsi, c.port_id) for c in open_before}

        fetched: list[Obs] = []
        if pairs:
            mmsis, port_ids = zip(*sorted(pairs), strict=True)
            rows = await conn.fetch(OBSERVATIONS, list(mmsis), list(port_ids), since)
            fetched = [
                Obs(r["mmsi"], r["port_id"], r["time"], r["dist_m"], r["sog"],
                    r["kmh_prev"], r["kmh_next"])
                for r in rows
            ]
        obs = plausible(fetched, MAX_KMH)

        result = detect(calls, obs, ports)

        if result.opened:
            await conn.executemany(INSERT_CALL, [
                (c.mmsi, c.port_id, c.approach_at, c.arrival_at, c.departure_at)
                for c in result.opened
            ])
        if result.updated:
            await conn.executemany(UPDATE_CALL, [
                (c.id, c.arrival_at, c.departure_at) for c in result.updated
            ])
        open_total = await conn.fetchval(COUNT_OPEN)

    seen = {(o.mmsi, o.port_id) for o in obs}
    arrived = sum(1 for c in result.opened if c.arrival_at is not None)
    departed = sum(1 for c in result.opened if c.departure_at is not None)
    for c in result.updated:
        was_arrival, was_departure = before[id(c)]
        arrived += was_arrival is None and c.arrival_at is not None
        departed += was_departure is None and c.departure_at is not None

    return {
        "window_min": since_minutes,
        "obs": len(obs),
        "gated": len(fetched) - len(obs),
        "pairs": len(pairs),
        "opened": len(result.opened),
        "arrived": arrived,
        "departed": departed,
        "open_total": open_total,
        "stale_open": sum(1 for c in open_before if (c.mmsi, c.port_id) not in seen),
        "took": time.monotonic() - started,
    }


async def main(args: argparse.Namespace) -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopping.set)
        except NotImplementedError:
            pass

    try:
        while True:
            try:
                s = await run_once(pool, args.since_minutes)
                log.info(
                    "portcalls window=%dm obs=%d gated=%d pairs=%d opened=%d arrived=%d "
                    "departed=%d open_total=%d stale_open=%d took=%.2fs",
                    s["window_min"], s["obs"], s["gated"], s["pairs"], s["opened"],
                    s["arrived"], s["departed"], s["open_total"], s["stale_open"], s["took"],
                )
            except Exception as exc:  # the periodic job must outlive a bad run
                if args.once:
                    raise
                log.error("run failed: %s", exc)
            if args.once or stopping.is_set():
                break
            try:
                await asyncio.wait_for(stopping.wait(), timeout=args.interval)
            except asyncio.TimeoutError:
                pass
    finally:
        await pool.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--since-minutes", type=int, default=15)
    p.add_argument("--interval", type=float, default=60.0)
    p.add_argument("--once", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
