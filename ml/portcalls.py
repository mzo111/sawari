"""Port-call detection over positions already measured against ports.

Deliberately pure: no I/O, no clock, no geodesy. Distances arrive already
computed by PostGIS; this module only decides when a call opens, arrives and
departs, so the rules are testable with synthetic tracks.

Rules:
  * first fix inside anchorage_m opens a call (approach_at)
  * first fix inside berth_m with sog < stopped_below_kn sets arrival_at
  * first fix outside anchorage_m after that sets departure_at and closes it
A call with arrival_at NULL is a candidate (passing traffic), not an error.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class Port:
    id: int
    anchorage_m: float
    berth_m: float


@dataclass(frozen=True)
class Obs:
    mmsi: int
    port_id: int
    time: datetime
    dist_m: float
    sog: float | None


@dataclass
class Call:
    mmsi: int
    port_id: int
    approach_at: datetime
    arrival_at: datetime | None = None
    departure_at: datetime | None = None
    id: int | None = None

    @property
    def is_open(self) -> bool:
        return self.departure_at is None


@dataclass
class Result:
    opened: list[Call] = field(default_factory=list)
    updated: list[Call] = field(default_factory=list)


def detect(
    calls: list[Call],
    obs: list[Obs],
    ports: dict[int, Port],
    stopped_below_kn: float = 0.5,
) -> Result:
    """Apply the rules to `obs` given the calls that already exist.

    `calls` should include every open call for the (mmsi, port) pairs in
    `obs`, plus any closed call whose interval overlaps the observation
    window - closed calls are what make re-processing the same positions a
    no-op. Each (mmsi, port) pair is independent: a vessel inside two
    anchorages gets two calls.
    """
    result = Result()
    touched: set[int] = set()

    existing: dict[tuple[int, int], list[Call]] = defaultdict(list)
    for call in calls:
        existing[(call.mmsi, call.port_id)].append(call)

    grouped: dict[tuple[int, int], list[Obs]] = defaultdict(list)
    for o in obs:
        grouped[(o.mmsi, o.port_id)].append(o)

    for key, group in grouped.items():
        port = ports[key[1]]
        closed = [c for c in existing.get(key, []) if not c.is_open]
        open_call = next((c for c in existing.get(key, []) if c.is_open), None)

        for o in sorted(group, key=lambda x: x.time):
            t = o.time
            if any(c.approach_at <= t <= c.departure_at for c in closed):
                continue

            inside = o.dist_m <= port.anchorage_m
            stopped_in_berth = (
                o.dist_m <= port.berth_m
                and o.sog is not None
                and o.sog < stopped_below_kn
            )

            if open_call is None:
                if inside:
                    open_call = Call(
                        o.mmsi, o.port_id, t,
                        arrival_at=t if stopped_in_berth else None,
                    )
                    result.opened.append(open_call)
                    touched.add(id(open_call))
                continue

            if t <= open_call.approach_at:
                continue

            if inside:
                if open_call.arrival_at is None and stopped_in_berth:
                    open_call.arrival_at = t
                    _mark_updated(result, touched, open_call)
            else:
                open_call.departure_at = t
                _mark_updated(result, touched, open_call)
                closed.append(open_call)
                open_call = None

    return result


def _mark_updated(result: Result, touched: set[int], call: Call) -> None:
    # Calls opened in this run already carry their changes; only pre-existing
    # calls need to be reported for UPDATE, and only once each.
    if id(call) not in touched:
        touched.add(id(call))
        result.updated.append(call)
