"""Evaluate the naive ETA baseline on a temporal holdout.

  python -m ml.eval [--holdout-frac 0.2]

Reads the shared dataset (ml.dataset), splits by arrival_at, and prints MAE
overall, by distance band, and by port. report() is reused by ml.train so
the model is scored on exactly the same tables.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from collections import defaultdict
from collections.abc import Callable

import asyncpg

from .baseline import SOG_FLOOR_KN, eta_hours, is_floored, mae, temporal_split
from .dataset import Row, load_rows
from .portcalls_job import MAX_KMH

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sawari:sawari@localhost:5432/sawari"
)

BANDS = (("0-5km", 0, 5000), ("5-10km", 5000, 10000), ("10-15km", 10000, float("inf")))

Predictor = Callable[[list[Row]], list[float]]


def baseline_predict(rows: list[Row]) -> list[float]:
    return [eta_hours(r.dist_m, r.sog) for r in rows]


def band(dist_m: float) -> str:
    for name, lo, hi in BANDS:
        if lo <= dist_m < hi:
            return name
    return BANDS[-1][0]


def fmt(hours: float | None) -> str:
    return "n/a" if hours is None else f"{hours:.3f} h ({hours * 60:.1f} min)"


def report(title: str, rows: list[Row], predict: Predictor) -> None:
    pairs = list(zip(predict(rows), [r.hours_to_arrival for r in rows], strict=True))
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
        rows = await load_rows(conn)
    finally:
        await conn.close()

    train, holdout, cutoff = temporal_split(rows, holdout_frac)
    assert len(train) + len(holdout) == len(rows)
    assert all(r.arrival_at < cutoff for r in train)
    assert all(r.arrival_at >= cutoff for r in holdout)
    assert not ({r.call_id for r in train} & {r.call_id for r in holdout})

    print(f"dataset: rows={len(rows)} calls={len({r.call_id for r in rows})} "
          f"gate=MAX_KMH {MAX_KMH:.0f} floor={SOG_FLOOR_KN:.0f} kn holdout_frac={holdout_frac}")
    print(f"temporal cutoff on arrival_at: {cutoff.isoformat() if cutoff else None}")
    report("holdout (arrival_at >= cutoff)", holdout, baseline_predict)
    report("train, reference only (arrival_at < cutoff)", train, baseline_predict)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--holdout-frac", type=float, default=0.2)
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args().holdout_frac))
