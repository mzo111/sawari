"""Tests for ingest.parser.

The parser is pure (no I/O, no clock), so these need no fixtures or mocks:
build a realistic AISStream payload, call the function, check the result.
"""

from datetime import datetime, timezone

from ingest.parser import (
    DropReason,
    Position,
    parse_position,
    parse_static,
    parse_time,
)

MMSI = 244660000  # Dutch-flagged, off the Maas approach to Rotterdam
TIME_UTC = "2026-09-06 05:30:14.123456789 +0000 UTC"
EXPECTED_TIME = datetime(2026, 9, 6, 5, 30, 14, 123456, tzinfo=timezone.utc)


def position_msg(**report):
    """A realistic AISStream PositionReport envelope. Keyword args override
    fields in Message.PositionReport; delete keys directly in the test when
    the point is that a field is absent."""
    msg = {
        "MessageType": "PositionReport",
        "MetaData": {
            "MMSI": MMSI,
            "ShipName": "NEDERLAND",
            "latitude": 51.95,
            "longitude": 4.05,
            "time_utc": TIME_UTC,
        },
        "Message": {
            "PositionReport": {
                "MessageID": 1,
                "RepeatIndicator": 0,
                "UserID": MMSI,
                "Valid": True,
                "NavigationalStatus": 0,
                "RateOfTurn": -128,
                "Sog": 12.3,
                "PositionAccuracy": True,
                "Longitude": 4.05,
                "Latitude": 51.95,
                "Cog": 245.6,
                "TrueHeading": 247,
                "Timestamp": 14,
                "SpecialManoeuvreIndicator": 0,
                "Spare": 0,
                "Raim": False,
                "CommunicationState": 0,
            }
        },
    }
    msg["Message"]["PositionReport"].update(report)
    return msg


def static_msg(**data):
    """A realistic AISStream ShipStaticData envelope, with the @-padded text
    fields exactly as the feed sends them."""
    msg = {
        "MessageType": "ShipStaticData",
        "MetaData": {
            "MMSI": MMSI,
            "ShipName": "NEDERLAND",
            "latitude": 51.95,
            "longitude": 4.05,
            "time_utc": TIME_UTC,
        },
        "Message": {
            "ShipStaticData": {
                "MessageID": 5,
                "RepeatIndicator": 0,
                "UserID": MMSI,
                "Valid": True,
                "AisVersion": 1,
                "ImoNumber": 9811000,
                "CallSign": "PBSV@@@",
                "Name": "NEDERLAND@@@@@@@@@@@",
                "Type": 70,
                "Dimension": {"A": 200, "B": 100, "C": 20, "D": 25},
                "FixType": 1,
                "Eta": {"Month": 9, "Day": 7, "Hour": 6, "Minute": 0},
                "MaximumStaticDraught": 11.5,
                "Destination": "NLRTM@@@@@@@@@@@@@@@",
                "Dte": False,
                "Spare": False,
            }
        },
    }
    msg["Message"]["ShipStaticData"].update(data)
    return msg


# ------------------------------------------------------------ DropReason


def test_bad_mmsi_out_of_range_low():
    pos, reason = parse_position(position_msg(UserID=12345))
    assert pos is None
    assert reason == DropReason.BAD_MMSI


def test_bad_mmsi_out_of_range_high():
    pos, reason = parse_position(position_msg(UserID=1_000_000_000))
    assert pos is None
    assert reason == DropReason.BAD_MMSI


def test_bad_mmsi_non_numeric():
    pos, reason = parse_position(position_msg(UserID="ABC"))
    assert pos is None
    assert reason == DropReason.BAD_MMSI


def test_bad_mmsi_missing_everywhere():
    msg = position_msg()
    del msg["Message"]["PositionReport"]["UserID"]
    del msg["MetaData"]["MMSI"]
    pos, reason = parse_position(msg)
    assert pos is None
    assert reason == DropReason.BAD_MMSI


def test_null_island():
    pos, reason = parse_position(position_msg(Latitude=0.0, Longitude=0.0))
    assert pos is None
    assert reason == DropReason.NULL_ISLAND


def test_out_of_range_latitude():
    pos, reason = parse_position(position_msg(Latitude=91.0))
    assert pos is None
    assert reason == DropReason.OUT_OF_RANGE_COORDS


def test_out_of_range_longitude():
    pos, reason = parse_position(position_msg(Longitude=181.0))
    assert pos is None
    assert reason == DropReason.OUT_OF_RANGE_COORDS


def test_impossible_speed_too_fast():
    pos, reason = parse_position(position_msg(Sog=60.0))
    assert pos is None
    assert reason == DropReason.IMPOSSIBLE_SPEED


def test_impossible_speed_negative():
    pos, reason = parse_position(position_msg(Sog=-1.0))
    assert pos is None
    assert reason == DropReason.IMPOSSIBLE_SPEED


def test_bad_timestamp_garbage():
    msg = position_msg()
    msg["MetaData"]["time_utc"] = "sometime yesterday"
    pos, reason = parse_position(msg)
    assert pos is None
    assert reason == DropReason.BAD_TIMESTAMP


def test_bad_timestamp_missing():
    msg = position_msg()
    del msg["MetaData"]["time_utc"]
    pos, reason = parse_position(msg)
    assert pos is None
    assert reason == DropReason.BAD_TIMESTAMP


def test_malformed_empty_dict():
    pos, reason = parse_position({})
    assert pos is None
    assert reason == DropReason.MALFORMED


def test_malformed_not_a_dict():
    pos, reason = parse_position(None)
    assert pos is None
    assert reason == DropReason.MALFORMED


def test_malformed_no_position_report():
    msg = position_msg()
    msg["Message"] = {"ShipStaticData": {}}
    pos, reason = parse_position(msg)
    assert pos is None
    assert reason == DropReason.MALFORMED


def test_malformed_missing_coords_everywhere():
    msg = position_msg()
    del msg["Message"]["PositionReport"]["Latitude"]
    del msg["Message"]["PositionReport"]["Longitude"]
    del msg["MetaData"]["latitude"]
    del msg["MetaData"]["longitude"]
    pos, reason = parse_position(msg)
    assert pos is None
    assert reason == DropReason.MALFORMED


# ------------------------------------------------------------ parse_time


def test_parse_time_nanoseconds_truncated_to_micros():
    ts = parse_time(TIME_UTC)
    assert ts == EXPECTED_TIME
    assert ts.tzinfo is timezone.utc


def test_parse_time_no_fractional_part():
    ts = parse_time("2026-09-06 05:30:14 +0000 UTC")
    assert ts == datetime(2026, 9, 6, 5, 30, 14, tzinfo=timezone.utc)


def test_parse_time_short_fraction_padded():
    ts = parse_time("2026-09-06 05:30:14.5 +0000 UTC")
    assert ts.microsecond == 500000


def test_parse_time_garbage_returns_none():
    assert parse_time("not a timestamp") is None


def test_parse_time_empty_returns_none():
    assert parse_time("") is None


# ------------------------------------------------------ AIS sentinels


def test_sog_sentinel_102_3_becomes_none():
    pos, reason = parse_position(position_msg(Sog=102.3))
    assert reason is None
    assert pos.sog is None


def test_heading_511_becomes_none():
    pos, reason = parse_position(position_msg(TrueHeading=511))
    assert reason is None
    assert pos.heading is None


def test_heading_out_of_range_becomes_none():
    pos, reason = parse_position(position_msg(TrueHeading=400))
    assert reason is None
    assert pos.heading is None


def test_heading_missing_becomes_none():
    msg = position_msg()
    del msg["Message"]["PositionReport"]["TrueHeading"]
    pos, reason = parse_position(msg)
    assert reason is None
    assert pos.heading is None


def test_cog_360_becomes_none():
    pos, reason = parse_position(position_msg(Cog=360.0))
    assert reason is None
    assert pos.cog is None


def test_cog_negative_becomes_none():
    pos, reason = parse_position(position_msg(Cog=-5.0))
    assert reason is None
    assert pos.cog is None


# ----------------------------------------------------------- parse_static


def test_static_at_padding_stripped():
    static = parse_static(static_msg())
    assert static.name == "NEDERLAND"
    assert static.call_sign == "PBSV"
    assert static.destination == "NLRTM"


def test_static_all_at_padding_becomes_none():
    static = parse_static(static_msg(Name="@@@@@@@@@@@@@@@@@@@@"))
    assert static.name is None


def test_static_dimensions_summed():
    static = parse_static(static_msg())
    assert static.length_m == 300.0
    assert static.width_m == 45.0


def test_static_missing_dimension_dict():
    msg = static_msg()
    del msg["Message"]["ShipStaticData"]["Dimension"]
    static = parse_static(msg)
    assert static.length_m is None
    assert static.width_m is None


def test_static_zero_values_become_none():
    static = parse_static(static_msg(
        ImoNumber=0,
        MaximumStaticDraught=0,
        Dimension={"A": 0, "B": 0, "C": 0, "D": 0},
    ))
    assert static.imo is None
    assert static.draught_m is None
    assert static.length_m is None
    assert static.width_m is None


def test_static_bad_mmsi_returns_none():
    assert parse_static(static_msg(UserID=12345)) is None


def test_static_malformed_returns_none():
    assert parse_static({}) is None
    assert parse_static(position_msg()) is None


# -------------------------------------------------------------- round trip


def test_valid_position_round_trip():
    msg = position_msg()
    # If the parser wrongly preferred MetaData coords this would be a drop.
    msg["MetaData"]["latitude"] = 99.0
    pos, reason = parse_position(msg)
    assert reason is None
    assert pos == Position(
        time=EXPECTED_TIME,
        mmsi=MMSI,
        lon=4.05,
        lat=51.95,
        sog=12.3,
        cog=245.6,
        heading=247,
        nav_status=0,
    )
    assert isinstance(pos.lon, float)
    assert isinstance(pos.heading, int)


def test_position_falls_back_to_metadata_coords_and_mmsi():
    msg = position_msg()
    del msg["Message"]["PositionReport"]["UserID"]
    del msg["Message"]["PositionReport"]["Latitude"]
    del msg["Message"]["PositionReport"]["Longitude"]
    pos, reason = parse_position(msg)
    assert reason is None
    assert pos.mmsi == MMSI
    assert pos.lat == 51.95
    assert pos.lon == 4.05
