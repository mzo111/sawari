"""AISStream message parsing and validation.

Deliberately pure: no network, no database, no clock. Everything here is a
function of its input, which is what makes Day 4's tests cheap to write.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

# --- validation bounds -------------------------------------------------------
MAX_SOG_KNOTS = 50.0        # nothing commercial goes faster; above this is noise
MMSI_MIN, MMSI_MAX = 100_000_000, 999_999_999
HEADING_UNAVAILABLE = 511


class DropReason:
    BAD_MMSI = "bad_mmsi"
    NULL_ISLAND = "null_island"
    OUT_OF_RANGE_COORDS = "out_of_range_coords"
    IMPOSSIBLE_SPEED = "impossible_speed"
    BAD_TIMESTAMP = "bad_timestamp"
    MALFORMED = "malformed"


@dataclass(frozen=True)
class Position:
    time: datetime
    mmsi: int
    lon: float
    lat: float
    sog: float | None
    cog: float | None
    heading: int | None
    nav_status: int | None


@dataclass(frozen=True)
class VesselStatic:
    mmsi: int
    name: str | None
    call_sign: str | None
    imo: int | None
    ship_type: int | None
    length_m: float | None
    width_m: float | None
    draught_m: float | None
    destination: str | None


def parse_time(raw: str) -> datetime | None:
    """AISStream MetaData.time_utc looks like:
    '2026-09-04 11:22:33.123456789 +0000 UTC'
    """
    if not raw:
        return None
    try:
        head = raw.split(" +")[0].split(" UTC")[0].strip()
        if "." in head:
            date_part, frac = head.split(".", 1)
            frac = frac[:6].ljust(6, "0")          # ns -> us
            head = f"{date_part}.{frac}"
            fmt = "%Y-%m-%d %H:%M:%S.%f"
        else:
            fmt = "%Y-%m-%d %H:%M:%S"
        return datetime.strptime(head, fmt).replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return None


def parse_position(msg: dict[str, Any]) -> tuple[Position | None, str | None]:
    """Returns (position, drop_reason). Exactly one of the two is None."""
    try:
        meta = msg["MetaData"]
        report = msg["Message"]["PositionReport"]
    except (KeyError, TypeError):
        return None, DropReason.MALFORMED

    mmsi = report.get("UserID") or meta.get("MMSI")
    try:
        mmsi = int(mmsi)
    except (TypeError, ValueError):
        return None, DropReason.BAD_MMSI
    if not (MMSI_MIN <= mmsi <= MMSI_MAX):
        return None, DropReason.BAD_MMSI

    lat = report.get("Latitude", meta.get("latitude"))
    lon = report.get("Longitude", meta.get("longitude"))
    if lat is None or lon is None:
        return None, DropReason.MALFORMED
    if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        return None, DropReason.OUT_OF_RANGE_COORDS
    if abs(lat) < 1e-6 and abs(lon) < 1e-6:
        return None, DropReason.NULL_ISLAND

    sog = report.get("Sog")
    if sog is not None:
        if sog >= 102.3:                 # AIS sentinel for "not available"
            sog = None
        elif sog < 0 or sog > MAX_SOG_KNOTS:
            return None, DropReason.IMPOSSIBLE_SPEED

    cog = report.get("Cog")
    if cog is not None and not (0.0 <= cog < 360.0):
        cog = None

    heading = report.get("TrueHeading")
    if heading is None or heading == HEADING_UNAVAILABLE or not (0 <= heading < 360):
        heading = None

    ts = parse_time(meta.get("time_utc", ""))
    if ts is None:
        return None, DropReason.BAD_TIMESTAMP

    return Position(
        time=ts,
        mmsi=mmsi,
        lon=float(lon),
        lat=float(lat),
        sog=float(sog) if sog is not None else None,
        cog=float(cog) if cog is not None else None,
        heading=int(heading) if heading is not None else None,
        nav_status=report.get("NavigationalStatus"),
    ), None


def parse_static(msg: dict[str, Any]) -> VesselStatic | None:
    try:
        meta = msg["MetaData"]
        data = msg["Message"]["ShipStaticData"]
    except (KeyError, TypeError):
        return None

    try:
        mmsi = int(data.get("UserID") or meta.get("MMSI"))
    except (TypeError, ValueError):
        return None
    if not (MMSI_MIN <= mmsi <= MMSI_MAX):
        return None

    dim = data.get("Dimension") or {}
    length = _sum_or_none(dim.get("A"), dim.get("B"))
    width = _sum_or_none(dim.get("C"), dim.get("D"))

    return VesselStatic(
        mmsi=mmsi,
        name=_clean(data.get("Name")),
        call_sign=_clean(data.get("CallSign")),
        imo=data.get("ImoNumber") or None,
        ship_type=data.get("Type"),
        length_m=length,
        width_m=width,
        draught_m=data.get("MaximumStaticDraught") or None,
        destination=_clean(data.get("Destination")),
    )


def _clean(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.replace("@", " ").strip()
    return cleaned or None


def _sum_or_none(a: Any, b: Any) -> float | None:
    if a is None or b is None:
        return None
    total = float(a) + float(b)
    return total or None
