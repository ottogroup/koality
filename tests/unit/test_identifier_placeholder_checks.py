"""Unit tests ensuring identifier_placeholder is respected by checks."""

import datetime as dt

import pytest

from koality.checks import (
    AverageCheck,
    CountCheck,
    DuplicateCheck,
    MatchRateCheck,
    MaxCheck,
    MinCheck,
    NullRatioCheck,
    OccurrenceCheck,
    RegexMatchCheck,
    RollingValuesInSetCheck,
)

pytestmark = pytest.mark.unit


def test_average_check_uses_identifier_placeholder() -> None:
    """AverageCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = AverageCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH",
    )
    assert chk.identifier == "PH"
    assert chk.identifier_column == "SHOP_ID"


def test_max_check_uses_identifier_placeholder() -> None:
    """MaxCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = MaxCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH2",
    )
    assert chk.identifier == "PH2"
    assert chk.identifier_column == "SHOP_ID"


def test_min_check_uses_identifier_placeholder() -> None:
    """MinCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = MinCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH3",
    )
    assert chk.identifier == "PH3"
    assert chk.identifier_column == "SHOP_ID"


def test_occurrence_check_uses_identifier_placeholder() -> None:
    """OccurrenceCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = OccurrenceCheck(
        database_accessor="",
        database_provider=None,
        max_or_min="max",
        table="t",
        check_column="col",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH4",
    )
    assert chk.identifier == "PH4"
    assert chk.identifier_column == "SHOP_ID"


def test_rolling_values_in_set_check_requires_date_and_uses_placeholder() -> None:
    """RollingValuesInSetCheck requires a date filter and uses identifier_placeholder."""
    # Provide a date filter to satisfy constructor requirements
    today = dt.datetime.now(tz=dt.UTC).date().isoformat()
    chk = RollingValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        value_set=["a"],
        filters={
            "partition_date": {"column": "DATE", "value": today, "type": "date"},
            "shop_id": {"type": "identifier"},
        },
        identifier_format="filter_name",
        identifier_placeholder="PH5",
    )
    assert chk.identifier == "PH5"
    assert chk.identifier_column == "SHOP_ID"


def test_values_in_set_check_uses_identifier_placeholder() -> None:
    """ValuesInSetCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = RollingValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        value_set=["a"],
        filters={
            "shop_id": {"type": "identifier"},
            "partition_date": {
                "column": "DATE",
                "value": dt.datetime.now(tz=dt.UTC).date().isoformat(),
                "type": "date",
            },
        },
        identifier_format="filter_name",
        identifier_placeholder="PH6",
    )
    assert chk.identifier == "PH6"
    assert chk.identifier_column == "SHOP_ID"


def test_regex_check_use_identifier_placeholder() -> None:
    """RegexMatchCheck and MatchRateCheck use identifier_placeholder for naming-only identifier filters."""
    regex_chk = RegexMatchCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        regex_to_match="^a",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH7",
    )
    assert regex_chk.identifier == "PH7"
    assert regex_chk.identifier_column == "SHOP_ID"


def test_matchrate_check_uses_identifier_placeholder() -> None:
    """MatchRateCheck uses identifier_placeholder for naming-only identifier filters."""
    # MatchRateCheck
    mr_chk = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="left",
        right_table="right",
        check_column="col",
        join_columns=["id"],
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH8",
    )
    assert mr_chk.identifier == "PH8"
    assert mr_chk.identifier_column == "SHOP_ID"


# CountCheck, DuplicateCheck, NullRatioCheck,
def test_count_check_uses_identifier_placeholder() -> None:
    """CountCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = CountCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH9",
    )
    assert chk.identifier == "PH9"
    assert chk.identifier_column == "SHOP_ID"


def test_duplicate_check_uses_identifier_placeholder() -> None:
    """DuplicateCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = DuplicateCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH10",
    )
    assert chk.identifier == "PH10"
    assert chk.identifier_column == "SHOP_ID"


def test_null_ratio_check_uses_identifier_placeholder() -> None:
    """NullRatioCheck uses identifier_placeholder for naming-only identifier filters."""
    chk = NullRatioCheck(
        database_accessor="",
        database_provider=None,
        table="t",
        check_column="col",
        filters={"shop_id": {"type": "identifier"}},
        identifier_format="filter_name",
        identifier_placeholder="PH11",
    )
    assert chk.identifier == "PH11"
    assert chk.identifier_column == "SHOP_ID"
