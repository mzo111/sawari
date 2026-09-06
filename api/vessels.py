from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Path, Query, Request

router = APIRouter(prefix="/vessels", tags=["vessels"])

MMSI_MIN, MMSI_MAX = 100_000_000, 999_999_999

# DISTINCT ON over the (mmsi, time DESC) unique index: Timescale plans this as
# a SkipScan, measured at 23 ms for the last page of ~7,500 active vessels.
ACTIVE_VESSELS = """
SELECT p.mmsi, v.name, v.ship_type, p.time,
       ST_Y(p.geom) AS lat, ST_X(p.geom) AS lon,
       p.sog, p.cog, p.heading, p.nav_status
FROM (
    SELECT DISTINCT ON (mmsi) mmsi, time, geom, sog, cog, heading, nav_status
    FROM positions
    WHERE time > now() - make_interval(mins => $1)
    ORDER BY mmsi, time DESC
) p
LEFT JOIN vessels v USING (mmsi)
ORDER BY p.mmsi
LIMIT $2 OFFSET $3
"""

TRACK = """
SELECT time, ST_Y(geom) AS lat, ST_X(geom) AS lon, sog, cog, heading, nav_status
FROM positions
WHERE mmsi = $1 AND time > now() - make_interval(hours => $2)
ORDER BY time DESC
LIMIT $3
"""


@router.get("")
async def list_vessels(
    request: Request,
    minutes: Annotated[int, Query(ge=1, le=1440)] = 10,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict:
    rows = await request.app.state.pool.fetch(ACTIVE_VESSELS, minutes, limit, offset)
    return {
        "minutes": minutes,
        "limit": limit,
        "offset": offset,
        "items": [dict(row) for row in rows],
    }


@router.get("/{mmsi}/track")
async def vessel_track(
    request: Request,
    mmsi: Annotated[int, Path(ge=MMSI_MIN, le=MMSI_MAX)],
    hours: Annotated[int, Query(ge=1, le=168)] = 24,
    limit: Annotated[int, Query(ge=1, le=5000)] = 1000,
) -> dict:
    rows = await request.app.state.pool.fetch(TRACK, mmsi, hours, limit)
    return {
        "mmsi": mmsi,
        "hours": hours,
        "limit": limit,
        "capped": len(rows) == limit,
        "points": [dict(row) for row in reversed(rows)],
    }
