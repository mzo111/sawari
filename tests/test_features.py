import math
from datetime import datetime, timedelta, timezone

import pytest

from ml.dataset import Row
from ml.features import (
    FEATURES,
    featurize,
    hour_utc,
    minutes_in_anchorage,
    signed_angle_diff,
)

T0 = datetime(2026, 9, 6, 6, 30, tzinfo=timezone.utc)


def make_row(**override):
    base = {
        "call_id": 1, "port": "Rotterdam",
        "approach_at": T0 - timedelta(minutes=45), "arrival_at": T0 + timedelta(minutes=20),
        "time": T0, "dist_m": 8000.0, "sog": 9.5, "hours_to_arrival": 20 / 60,
        "cog": 270.0, "bearing_deg": 280.0, "ship_type": 70,
        "length_m": 200.0, "width_m": 32.0, "draught_m": 11.5,
    }
    base.update(override)
    return Row(**base)


# ------------------------------------------------------- signed_angle_diff


def test_wrap_across_north_is_a_small_positive_angle():
    assert signed_angle_diff(10.0, 350.0) == pytest.approx(20.0)


def test_wrap_across_north_the_other_way_is_a_small_negative_angle():
    assert signed_angle_diff(350.0, 10.0) == pytest.approx(-20.0)


def test_equal_headings_give_zero():
    assert signed_angle_diff(123.4, 123.4) == 0.0


def test_opposite_headings_give_180_in_magnitude():
    assert abs(signed_angle_diff(0.0, 180.0)) == 180.0
    assert abs(signed_angle_diff(180.0, 0.0)) == 180.0


def test_wrap_just_either_side_of_zero():
    assert signed_angle_diff(359.9, 0.1) == pytest.approx(-0.2)
    assert signed_angle_diff(0.1, 359.9) == pytest.approx(0.2)


def test_result_always_in_half_open_range():
    for a in range(0, 360, 7):
        for b in range(0, 360, 11):
            d = signed_angle_diff(float(a), float(b))
            assert -180.0 <= d < 180.0


def test_missing_heading_gives_nan():
    assert math.isnan(signed_angle_diff(None, 10.0))
    assert math.isnan(signed_angle_diff(10.0, None))


# --------------------------------------------------------------- featurize


def test_featurize_matches_feature_list_in_length_and_order():
    values = featurize(make_row())
    assert len(values) == len(FEATURES)
    named = dict(zip(FEATURES, values, strict=True))
    assert named["dist_m"] == 8000.0
    assert named["sog"] == 9.5
    assert named["bearing_minus_cog"] == pytest.approx(10.0)
    assert named["ship_type"] == 70.0
    assert named["length_m"] == 200.0


def test_missing_draught_becomes_nan():
    named = dict(zip(FEATURES, featurize(make_row(draught_m=None)), strict=True))
    assert math.isnan(named["draught_m"])
    assert not math.isnan(named["length_m"])


def test_missing_cog_nans_cog_and_the_bearing_difference():
    named = dict(zip(FEATURES, featurize(make_row(cog=None)), strict=True))
    assert math.isnan(named["cog"])
    assert math.isnan(named["bearing_minus_cog"])


def test_hour_utc_is_fractional_and_converts_timezones():
    assert hour_utc(T0) == pytest.approx(6.5)
    plus_two = datetime(2026, 9, 6, 8, 30, tzinfo=timezone(timedelta(hours=2)))
    assert hour_utc(plus_two) == pytest.approx(6.5)


def test_minutes_in_anchorage_counts_from_approach():
    assert minutes_in_anchorage(make_row()) == pytest.approx(45.0)
    named = dict(zip(FEATURES, featurize(make_row()), strict=True))
    assert named["minutes_in_anchorage"] == pytest.approx(45.0)
