"""Tests for ml.portcalls with synthetic tracks.

Distances are given directly (as PostGIS would supply them), so a track is
just (minutes, metres from port, speed) triples.
"""

from datetime import datetime, timedelta, timezone

from ml.portcalls import Call, Obs, Port, detect, plausible

T0 = datetime(2026, 9, 6, 6, 0, tzinfo=timezone.utc)
PORTS = {1: Port(1, 15000, 3000), 2: Port(2, 15000, 3000)}
MMSI = 244660000
OTHER = 245053000


def at(minutes):
    return T0 + timedelta(minutes=minutes)


def track(points, mmsi=MMSI, port_id=1):
    return [Obs(mmsi, port_id, at(m), d, s) for m, d, s in points]


def run(obs, calls=()):
    return detect(list(calls), obs, PORTS)


# ------------------------------------------------------------ single calls


def test_passing_traffic_is_a_candidate_with_departure():
    result = run(track([
        (0, 20000, 12), (5, 12000, 12), (10, 8000, 12), (15, 12000, 12), (20, 20000, 12),
    ]))
    assert len(result.opened) == 1
    call = result.opened[0]
    assert call.approach_at == at(5)
    assert call.arrival_at is None
    assert call.departure_at == at(20)
    assert result.updated == []


def test_full_call_sets_arrival_at_first_stopped_berth_fix():
    result = run(track([
        (0, 20000, 12), (5, 10000, 8), (10, 2000, 3.0),
        (15, 1500, 0.2), (20, 1500, 0.1), (25, 10000, 6), (30, 20000, 12),
    ]))
    call = result.opened[0]
    assert call.approach_at == at(5)
    assert call.arrival_at == at(15)
    assert call.departure_at == at(30)


def test_moving_through_berth_radius_is_not_an_arrival():
    result = run(track([(0, 10000, 10), (5, 2000, 10), (10, 10000, 10), (15, 20000, 10)]))
    assert result.opened[0].arrival_at is None
    assert result.opened[0].departure_at == at(15)


def test_stopped_inside_anchorage_but_outside_berth_is_not_an_arrival():
    result = run(track([(0, 10000, 5), (5, 5000, 0.1), (10, 5000, 0.0)]))
    call = result.opened[0]
    assert call.arrival_at is None
    assert call.departure_at is None


def test_unknown_speed_never_counts_as_stopped():
    result = run(track([(0, 10000, 5), (5, 1000, None), (10, 1000, None)]))
    assert result.opened[0].arrival_at is None


def test_first_fix_already_stopped_in_berth_arrives_on_approach():
    result = run(track([(0, 1000, 0.0)]))
    call = result.opened[0]
    assert call.approach_at == at(0)
    assert call.arrival_at == at(0)


def test_outside_fixes_without_an_open_call_do_nothing():
    result = run(track([(0, 20000, 10), (5, 30000, 10)]))
    assert result.opened == []
    assert result.updated == []


def test_departure_after_ais_gap_is_first_outside_fix():
    result = run(track([(0, 10000, 8), (5, 2000, 0.1), (60, 40000, 12)]))
    call = result.opened[0]
    assert call.arrival_at == at(5)
    assert call.departure_at == at(60)


# --------------------------------------------------------------- re-entry


def test_reentry_after_departure_opens_a_second_call():
    result = run(track([(0, 10000, 10), (5, 20000, 10), (10, 10000, 10), (15, 20000, 10)]))
    assert len(result.opened) == 2
    first, second = result.opened
    assert (first.approach_at, first.departure_at) == (at(0), at(5))
    assert (second.approach_at, second.departure_at) == (at(10), at(15))
    assert second.approach_at > first.departure_at


# ---------------------------------------------- continuing from a prior run


def test_open_call_from_previous_run_gets_arrival():
    existing = Call(MMSI, 1, at(0), id=7)
    result = run(track([(5, 1000, 0.1)]), calls=[existing])
    assert result.opened == []
    assert result.updated == [existing]
    assert existing.arrival_at == at(5)
    assert existing.departure_at is None


def test_open_call_from_previous_run_gets_departure():
    existing = Call(MMSI, 1, at(0), arrival_at=at(2), id=7)
    result = run(track([(5, 20000, 10)]), calls=[existing])
    assert result.updated == [existing]
    assert existing.departure_at == at(5)


def test_fixes_at_or_before_open_call_approach_are_ignored():
    existing = Call(MMSI, 1, at(10), id=7)
    result = run(track([(5, 1000, 0.0), (10, 1000, 0.0)]), calls=[existing])
    assert result.opened == []
    assert result.updated == []
    assert existing.arrival_at is None


def test_closed_call_covering_the_window_blocks_a_duplicate_open():
    existing = Call(MMSI, 1, at(0), departure_at=at(10), id=7)
    result = run(track([(2, 5000, 3), (8, 5000, 3)]), calls=[existing])
    assert result.opened == []
    assert result.updated == []


def test_replaying_the_same_observations_is_a_noop():
    full = track([(0, 10000, 8), (5, 1500, 0.1), (10, 20000, 10)])
    first = run(full)
    again = run(full, calls=first.opened)
    assert again.opened == [] and again.updated == []

    still_open = track([(0, 10000, 8), (5, 1500, 0.1)])
    first = run(still_open)
    again = run(still_open, calls=first.opened)
    assert again.opened == [] and again.updated == []


# --------------------------------------------------- two ports, two vessels


def test_vessel_inside_two_anchorages_gets_two_calls_one_arrival():
    near_port_1 = track([
        (0, 14000, 8), (5, 10000, 8), (10, 2000, 0.1), (15, 10000, 8), (20, 20000, 8),
    ], port_id=1)
    near_port_2 = track([
        (0, 14000, 8), (5, 14500, 8), (10, 14000, 0.1), (15, 14500, 8), (20, 20000, 8),
    ], port_id=2)
    result = run(near_port_1 + near_port_2)
    by_port = {c.port_id: c for c in result.opened}
    assert set(by_port) == {1, 2}
    assert by_port[1].arrival_at == at(10)
    assert by_port[2].arrival_at is None
    assert by_port[1].departure_at == at(20)
    assert by_port[2].departure_at == at(20)


def test_interleaved_vessels_are_independent():
    passing = track([(0, 10000, 10), (10, 20000, 10)], mmsi=MMSI)
    berthing = track([(0, 10000, 5), (5, 1000, 0.0), (10, 20000, 8)], mmsi=OTHER)
    interleaved = [passing[0], berthing[0], berthing[2], passing[1], berthing[1]]
    result = run(interleaved)
    by_mmsi = {c.mmsi: c for c in result.opened}
    assert by_mmsi[MMSI].arrival_at is None
    assert by_mmsi[MMSI].departure_at == at(10)
    assert by_mmsi[OTHER].arrival_at == at(5)
    assert by_mmsi[OTHER].departure_at == at(10)


# ------------------------------------------------------ plausibility gate


def fix(minutes, dist_m=10000, sog=8.0, prev=None, nxt=None):
    return Obs(MMSI, 1, at(minutes), dist_m, sog, prev, nxt)


def test_gate_keeps_fixes_with_unknown_speeds():
    obs = [fix(0), fix(5, prev=20.0), fix(10, prev=20.0)]
    assert plausible(obs, 60.0) == obs


def test_gate_drops_on_previous_jump():
    assert plausible([fix(5, prev=61.0)], 60.0) == []


def test_gate_drops_on_next_jump():
    assert plausible([fix(5, nxt=61.0)], 60.0) == []


def test_gate_threshold_is_inclusive():
    kept = fix(5, prev=60.0, nxt=60.0)
    assert plausible([kept, fix(10, prev=60.01)], 60.0) == [kept]


def test_gate_drops_every_fix_of_an_alternating_collision():
    obs = [
        fix(0, nxt=4000.0),
        fix(2, prev=4000.0, nxt=4000.0),
        fix(4, prev=4000.0, nxt=4000.0),
        fix(6, prev=4000.0),
    ]
    assert plausible(obs, 60.0) == []


def test_gate_drops_spike_and_both_neighbours():
    obs = [
        fix(0, nxt=20.0),
        fix(5, prev=20.0, nxt=900.0),
        fix(10, prev=900.0, nxt=900.0),
        fix(15, prev=900.0, nxt=20.0),
        fix(20, prev=20.0, nxt=20.0),
        fix(25, prev=20.0),
    ]
    assert plausible(obs, 60.0) == [obs[0], obs[4], obs[5]]


def test_collided_track_yields_no_calls_after_gate():
    # The Zeebrugge shape: berthed, 159 km away 2 min later, back again.
    obs = [
        fix(0, 1000, 0.1, nxt=4000.0),
        fix(2, 159000, 15.5, prev=4000.0, nxt=2500.0),
        fix(6, 1000, 0.1, prev=2500.0),
    ]
    ungated = detect([], obs, PORTS)
    assert len(ungated.opened) == 2
    gated = detect([], plausible(obs, 60.0), PORTS)
    assert gated.opened == [] and gated.updated == []


def test_track_with_one_spike_still_yields_one_clean_call():
    obs = [
        fix(0, 10000, 8.0, nxt=20.0),
        fix(5, 2000, 0.1, prev=20.0, nxt=900.0),
        fix(10, 200000, 12.0, prev=900.0, nxt=900.0),
        fix(15, 2000, 0.1, prev=900.0, nxt=20.0),
        fix(20, 2000, 0.1, prev=20.0, nxt=25.0),
        fix(25, 20000, 12.0, prev=25.0),
    ]
    assert len(detect([], obs, PORTS).opened) == 2
    result = detect([], plausible(obs, 60.0), PORTS)
    assert len(result.opened) == 1
    call = result.opened[0]
    assert call.approach_at == at(0)
    assert call.arrival_at == at(20)   # the fix at 5 was a spike neighbour
    assert call.departure_at == at(25)


def test_gate_preserves_order():
    obs = [fix(0), fix(5, prev=10.0), fix(10, prev=10.0)]
    assert [o.time for o in plausible(obs, 60.0)] == [at(0), at(5), at(10)]
