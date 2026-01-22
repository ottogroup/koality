"""Unit tests ensuring identifier_placeholder is respected by checks."""

import datetime as dt

import pytest

from koality.checks import (
    AverageCheck,
    MaxCheck,
    MinCheck,
    OccurrenceCheck,
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
