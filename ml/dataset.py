"""The labelled ETA dataset - one definition shared by ml.eval and ml.train.

Every in-anchorage fix strictly before arrival, on port calls that were
actually observed approaching, gated by the same plausibility rule the
detector used to build the labels. Distances and bearings come from PostGIS.
Read-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import asyncpg

from .portcalls_job import MAX_KMH


@dataclass(frozen=True)
class Row:
    call_id: int
    port: str
    approach_at: datetime
    arrival_at: datetime
    time: datetime
    dist_m: float
    sog: float | None
    hours_to_arrival: float
    cog: float | None = None
    bearing_deg: float | None = None
    ship_type: int | None = None
    length_m: float | None = None
    width_m: float | None = None
    draught_m: float | None = None
    mmsi: int | None = None  # for splits and overlap counts, never a feature


# The lag/lead gate mirrors ml.portcalls.plausible() so feature rows are the
# fixes the labels came from. ST_Azimuth is NULL when the fix is exactly on
# the port point.
DATASET = """
WITH labelled AS (
    SELECT c.id AS call_id, c.mmsi, c.approach_at, c.arrival_at,
           p.name AS port, p.geom AS pgeom, p.anchorage_radius_m AS r,
           v.ship_type, v.length_m, v.width_m, v.draught_m
    FROM port_calls c
    JOIN ports p ON p.id = c.port_id
    LEFT JOIN vessels v ON v.mmsi = c.mmsi
    WHERE c.arrival_at IS NOT NULL AND c.approach_at < c.arrival_at
), fixes AS (
    SELECT mmsi, time, geom, sog, cog,
           ST_Distance(geom::geography, (lag(geom) OVER w)::geography)
             / NULLIF(EXTRACT(EPOCH FROM (time - lag(time) OVER w)), 0) * 3.6 AS kmh_prev,
           ST_Distance(geom::geography, (lead(geom) OVER w)::geography)
             / NULLIF(EXTRACT(EPOCH FROM (lead(time) OVER w - time)), 0) * 3.6 AS kmh_next
    FROM positions
    WHERE mmsi IN (SELECT mmsi FROM labelled)
      AND time >= (SELECT min(approach_at) FROM labelled)
    WINDOW w AS (PARTITION BY mmsi ORDER BY time)
)
SELECT l.call_id, l.mmsi, l.port, l.approach_at, l.arrival_at, f.time, f.sog, f.cog,
       ST_Distance(l.pgeom::geography, f.geom::geography) AS dist_m,
       degrees(ST_Azimuth(f.geom::geography, l.pgeom::geography)) AS bearing_deg,
       l.ship_type, l.length_m, l.width_m, l.draught_m,
       (EXTRACT(EPOCH FROM (l.arrival_at - f.time)) / 3600.0)::float8 AS hours_to_arrival
FROM labelled l
JOIN fixes f ON f.mmsi = l.mmsi AND f.time >= l.approach_at AND f.time < l.arrival_at
WHERE ST_DWithin(l.pgeom::geography, f.geom::geography, l.r)
  AND COALESCE(f.kmh_prev, 0) <= $1 AND COALESCE(f.kmh_next, 0) <= $1
ORDER BY l.arrival_at, l.call_id, f.time
"""


async def load_rows(conn: asyncpg.Connection) -> list[Row]:
    records = await conn.fetch(DATASET, MAX_KMH)
    return [
        Row(
            call_id=r["call_id"], port=r["port"],
            approach_at=r["approach_at"], arrival_at=r["arrival_at"], time=r["time"],
            dist_m=r["dist_m"], sog=r["sog"], hours_to_arrival=r["hours_to_arrival"],
            cog=r["cog"], bearing_deg=r["bearing_deg"], ship_type=r["ship_type"],
            length_m=r["length_m"], width_m=r["width_m"], draught_m=r["draught_m"],
            mmsi=r["mmsi"],
        )
        for r in records
    ]
