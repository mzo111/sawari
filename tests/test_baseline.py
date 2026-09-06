from datetime import datetime, timedelta, timezone

import pytest

from ml.baseline import SOG_FLOOR_KN, eta_hours, is_floored, mae, temporal_split
from ml.dataset import Row

T0 = datetime(2026, 9, 6, 6, 0, tzinfo=timezone.utc)


def at(minutes):
    return T0 + timedelta(minutes=minutes)


def row(call_id, arrival_minutes, minutes_before=10, port="Rotterdam"):
    return Row(
        call_id=call_id, port=port,
        approach_at=at(arrival_minutes - 60),
        arrival_at=at(arrival_minutes), time=at(arrival_minutes - minutes_before),
        dist_m=5000.0, sog=8.0, hours_to_arrival=minutes_before / 60,
    )


# --------------------------------------------------------------- eta_hours


def test_one_nautical_mile_at_one_knot_is_one_hour():
    assert eta_hours(1852.0, 1.0) == pytest.approx(1.0)


def test_ten_miles_at_ten_knots_is_one_hour():
    assert eta_hours(18520.0, 10.0) == pytest.approx(1.0)


def test_zero_distance_is_zero_hours():
    assert eta_hours(0.0, 12.0) == 0.0


def test_zero_speed_is_clamped_to_the_floor():
    assert eta_hours(5000.0, 0.0) == pytest.approx(5000.0 / 1852.0 / SOG_FLOOR_KN)


def test_jitter_speed_gets_the_same_answer_as_zero():
    assert eta_hours(5000.0, 0.3) == eta_hours(5000.0, 0.0)


def test_unknown_speed_is_treated_as_stopped():
    assert eta_hours(5000.0, None) == eta_hours(5000.0, 0.0)


def test_speed_above_the_floor_is_not_clamped():
    assert eta_hours(5000.0, 1.5) == pytest.approx(5000.0 / 1852.0 / 1.5)
    assert not is_floored(1.5)
    assert is_floored(0.9) and is_floored(None)


# --------------------------------------------------------------------- mae


def test_mae_is_mean_absolute_error():
    assert mae([(1.0, 2.0), (3.0, 1.0)]) == pytest.approx(1.5)


def test_mae_of_nothing_is_none():
    assert mae([]) is None


# ---------------------------------------------------------- temporal_split


def test_split_holds_out_the_latest_calls_by_arrival():
    rows = [row(c, arrival_minutes=c * 10) for c in range(1, 6)]
    train, holdout, cutoff = temporal_split(rows, holdout_frac=0.2)
    assert {r.call_id for r in train} == {1, 2, 3, 4}
    assert {r.call_id for r in holdout} == {5}
    assert cutoff == at(50)


def test_no_call_is_on_both_sides():
    rows = [row(c, arrival_minutes=c * 10, minutes_before=m) for c in range(1, 11) for m in (30, 20, 10)]
    train, holdout, _ = temporal_split(rows, holdout_frac=0.3)
    assert not ({r.call_id for r in train} & {r.call_id for r in holdout})
    assert len(train) + len(holdout) == len(rows)


def test_ties_on_arrival_at_stay_together_in_the_holdout():
    rows = [row(1, 10), row(2, 20), row(3, 30), row(4, 30), row(5, 30)]
    train, holdout, cutoff = temporal_split(rows, holdout_frac=0.2)
    assert cutoff == at(30)
    assert {r.call_id for r in holdout} == {3, 4, 5}
    assert {r.call_id for r in train} == {1, 2}


def test_input_order_does_not_matter():
    rows = [row(c, arrival_minutes=c * 10) for c in (3, 1, 5, 2, 4)]
    _, holdout, cutoff = temporal_split(rows, holdout_frac=0.2)
    assert {r.call_id for r in holdout} == {5}
    assert cutoff == min(r.arrival_at for r in holdout)


def test_empty_input_splits_to_nothing():
    assert temporal_split([], 0.2) == ([], [], None)
