"""Evaluate the naive ETA baseline on a temporal holdout.

  python -m ml.eval [--holdout-frac 0.2]

Reads port_calls x positions (never writes), applies the same plausibility
gate the detector used to build the labels, splits by arrival_at, and prints
MAE overall, by distance band, and by port.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from collections import defaultdict

import asyncpg

from .baseline import SOG_FLOOR_KN, Row, eta_hours, is_floored, mae, temporal_split
from .portcalls_job import MAX_KMH

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sawari:sawari@localhost:5432/sawari"
)

BANDS = (("0-5km", 0, 5000), ("5-10km", 5000, 10000), ("10-15km", 10000, float("inf")))

# Every in-anchorage fix strictly before arrival, on calls that were actually
# observed approaching. The lag/lead gate mirrors ml.portcalls.plausible() so
# feature rows are the fixes the labels came from.
DATASET = """
WITH labelled AS (
    SELECT c.id AS call_id, c.mmsi, c.approach_at, c.arrival_at,
           p.name AS port, p.geom AS pgeom, p.anchorage_radius_m AS r
    FROM port_calls c JOIN ports p ON p.id = c.port_id
    WHERE c.arrival_at IS NOT NULL AND c.approach_at < c.arrival_at
), fixes AS (
    SELECT mmsi, time, geom, sog,
           ST_Distance(geom::geography, (lag(geom) OVER w)::geography)
             / NULLIF(EXTRACT(EPOCH FROM (time - lag(time) OVER w)), 0) * 3.6 AS kmh_prev,
           ST_Distance(geom::geography, (lead(geom) OVER w)::geography)
             / NULLIF(EXTRACT(EPOCH FROM (lead(time) OVER w - time)), 0) * 3.6 AS kmh_next
    FROM positions
    WHERE mmsi IN (SELECT mmsi FROM labelled)
      AND time >= (SELECT min(approach_at) FROM labelled)
    WINDOW w AS (PARTITION BY mmsi ORDER BY time)
)
SELECT l.call_id, l.port, l.arrival_at, f.time, f.sog,
       ST_Distance(l.pgeom::geography, f.geom::geography) AS dist_m,
       (EXTRACT(EPOCH FROM (l.arrival_at - f.time)) / 3600.0)::float8 AS hours_to_arrival
FROM labelled l
JOIN fixes f ON f.mmsi = l.mmsi AND f.time >= l.approach_at AND f.time < l.arrival_at
WHERE ST_DWithin(l.pgeom::geography, f.geom::geography, l.r)
  AND COALESCE(f.kmh_prev, 0) <= $1 AND COALESCE(f.kmh_next, 0) <= $1
ORDER BY l.arrival_at, l.call_id, f.time
"""


def band(dist_m: float) -> str:
    for name, lo, hi in BANDS:
        if lo <= dist_m < hi:
            return name
    return BANDS[-1][0]


def fmt(hours: float | None) -> str:
    return "n/a" if hours is None else f"{hours:.3f} h ({hours * 60:.1f} min)"


def report(title: str, rows: list[Row]) -> None:
    pairs = [(eta_hours(r.dist_m, r.sog), r.hours_to_arrival) for r in rows]
    calls = len({r.call_id for r in rows})
    print(f"\n== {title}: rows={len(rows)} calls={calls}")
    print(f"   MAE, all rows:            {fmt(mae(pairs))}")

    moving = [(p, y) for (p, y), r in zip(pairs, rows, strict=True) if not is_floored(r.sog)]
    floored = len(rows) - len(moving)
    share = 100.0 * floored / len(rows) if rows else 0.0
    print(f"   MAE, sog >= {SOG_FLOOR_KN:.0f} kn only: {fmt(mae(moving))}  "
          f"[floored rows={floored}, {share:.1f}%]")

    by_band: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for (p, y), r in zip(pairs, rows, strict=True):
        by_band[band(r.dist_m)].append((p, y))
    print("   by distance band:")
    for name, _, _ in BANDS:
        print(f"     {name:>8}  n={len(by_band[name]):5d}  MAE={fmt(mae(by_band[name]))}")

    by_port: dict[str, list[tuple[float, float]]] = defaultdict(list)
    port_calls: dict[str, set[int]] = defaultdict(set)
    for (p, y), r in zip(pairs, rows, strict=True):
        by_port[r.port].append((p, y))
        port_calls[r.port].add(r.call_id)
    print("   by port:")
    for port in sorted(by_port, key=lambda k: -len(by_port[k])):
        print(f"     {port:>14}  n={len(by_port[port]):5d}  calls={len(port_calls[port]):3d}  "
              f"MAE={fmt(mae(by_port[port]))}")


async def main(holdout_frac: float) -> None:
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        records = await conn.fetch(DATASET, MAX_KMH)
    finally:
        await conn.close()

    rows = [
        Row(r["call_id"], r["port"], r["arrival_at"], r["time"],
            r["dist_m"], r["sog"], r["hours_to_arrival"])
        for r in records
    ]
    train, holdout, cutoff = temporal_split(rows, holdout_frac)

    assert len(train) + len(holdout) == len(rows)
    assert all(r.arrival_at < cutoff for r in train)
    assert all(r.arrival_at >= cutoff for r in holdout)
    assert not ({r.call_id for r in train} & {r.call_id for r in holdout})

    print(f"dataset: rows={len(rows)} calls={len({r.call_id for r in rows})} "
          f"gate=MAX_KMH {MAX_KMH:.0f} floor={SOG_FLOOR_KN:.0f} kn holdout_frac={holdout_frac}")
    print(f"temporal cutoff on arrival_at: {cutoff.isoformat() if cutoff else None}")
    report("holdout (arrival_at >= cutoff)", holdout)
    report("train, reference only (arrival_at < cutoff)", train)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--holdout-frac", type=float, default=0.2)
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args().holdout_frac))
