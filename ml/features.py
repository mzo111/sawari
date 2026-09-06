"""Feature engineering for the ETA model. Pure: no I/O, no clock.

Missing values become NaN and are left to XGBoost, which learns a default
branch for them at every split. No imputation.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .dataset import Row

FEATURES = [
    "dist_m",
    "sog",
    "cog",
    "bearing_minus_cog",
    "ship_type",
    "length_m",
    "width_m",
    "draught_m",
    "hour_utc",
    "minutes_in_anchorage",
]

NAN = float("nan")


def signed_angle_diff(a: float | None, b: float | None) -> float:
    """a - b folded into [-180, 180): 10 vs 350 is +20, not -340."""
    if a is None or b is None:
        return NAN
    return ((a - b + 180.0) % 360.0) - 180.0


def hour_utc(t: datetime) -> float:
    t = t.astimezone(timezone.utc)
    return t.hour + t.minute / 60.0 + t.second / 3600.0


def minutes_in_anchorage(row: Row) -> float:
    return (row.time - row.approach_at).total_seconds() / 60.0


def _num(x: float | None) -> float:
    return NAN if x is None else float(x)


def featurize(row: Row) -> list[float]:
    return [
        _num(row.dist_m),
        _num(row.sog),
        _num(row.cog),
        signed_angle_diff(row.bearing_deg, row.cog),
        _num(row.ship_type),
        _num(row.length_m),
        _num(row.width_m),
        _num(row.draught_m),
        hour_utc(row.time),
        minutes_in_anchorage(row),
    ]
