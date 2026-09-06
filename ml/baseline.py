"""Naive ETA: distance to the port divided by current speed. No training.

Pure: no I/O, no clock, no geodesy. Distances arrive already measured by
PostGIS (ST_Distance on geography), so this module has nothing to get wrong
about the shape of the earth.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .dataset import Row

METRES_PER_NM = 1852.0

# Below this the reported speed is GPS jitter on a ship that isn't making way
# (the detector calls < 0.5 kn "stopped" for the same reason). Dividing by
# jitter gives arbitrary hours; clamping gives a finite, pessimistic answer
# that a real model has to beat.
SOG_FLOOR_KN = 1.0


def eta_hours(dist_m: float, sog_kn: float | None) -> float:
    speed = max(sog_kn or 0.0, SOG_FLOOR_KN)
    return (dist_m / METRES_PER_NM) / speed


def is_floored(sog_kn: float | None) -> bool:
    return (sog_kn or 0.0) < SOG_FLOOR_KN


def mae(pairs: list[tuple[float, float]]) -> float | None:
    if not pairs:
        return None
    return sum(abs(predicted - actual) for predicted, actual in pairs) / len(pairs)


def temporal_split(
    rows: list[Row], holdout_frac: float = 0.2
) -> tuple[list[Row], list[Row], datetime | None]:
    """Split by port call on arrival_at: the latest calls are the holdout.

    Never by row - one call yields dozens of rows, and a random split would
    put the same approach on both sides. Ties on arrival_at stay together.
    """
    calls = sorted({(r.call_id, r.arrival_at) for r in rows}, key=lambda c: (c[1], c[0]))
    idx = int(len(calls) * (1.0 - holdout_frac))
    if not calls or idx >= len(calls):
        return list(rows), [], None
    cutoff = calls[idx][1]
    train = [r for r in rows if r.arrival_at < cutoff]
    holdout = [r for r in rows if r.arrival_at >= cutoff]
    return train, holdout, cutoff


def group_split(
    rows: list[Row], holdout_frac: float = 0.2
) -> tuple[list[Row], list[Row], datetime | None]:
    """temporal_split, then purge from training every vessel that appears in
    the holdout. The holdout is unchanged, so the only thing that moves is
    what the model was allowed to see - that isolates vessel leakage."""
    train, holdout, cutoff = temporal_split(rows, holdout_frac)
    holdout_mmsi = {r.mmsi for r in holdout}
    return [r for r in train if r.mmsi not in holdout_mmsi], holdout, cutoff
