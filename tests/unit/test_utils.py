"""Unit tests for utility functions."""

import datetime as dt

import pytest

from koality.utils import parse_date, to_set

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("input_date", "offset", "expected"),
    [
        ("today", 0, dt.datetime.now(tz=dt.UTC).date().isoformat()),
        ("yesterday", 0, (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()),
        ("tomorrow", 0, (dt.datetime.now(tz=dt.UTC).date() + dt.timedelta(days=1)).isoformat()),
        ("today", 1, (dt.datetime.now(tz=dt.UTC).date() + dt.timedelta(days=1)).isoformat()),
        ("yesterday", 1, dt.datetime.now(tz=dt.UTC).date().isoformat()),
        ("tomorrow", 1, (dt.datetime.now(tz=dt.UTC).date() + dt.timedelta(days=2)).isoformat()),
        ("today", -2, (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=2)).isoformat()),
        ("yesterday", -2, (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=3)).isoformat()),
        ("tomorrow", -2, (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()),
        ("19901003", 0, "1990-10-03"),
        ("19901003", 5, "1990-10-08"),
        ("1990-10-03", 0, "1990-10-03"),
    ],
)
def test_parse_date(input_date: str, offset: int, expected: str) -> None:
    """Test parse_date function with various date inputs and offsets."""
    assert expected == parse_date(input_date, offset)


@pytest.mark.parametrize(
    ("test_input", "expected"),
    [
        ('("toys", "clothing")', {"clothing", "toys"}),
        ('("toys")', {"toys"}),
        ('"toys"', {"toys"}),
        ("toys", {"toys"}),
        ('("toys", "toys", "clothing")', {"clothing", "toys"}),
        ('("clothing", "toys")', {"clothing", "toys"}),
        (True, {True}),
        (1, {1}),
        (["toys"], {"toys"}),
        ({"toys"}, {"toys"}),
    ],
)
def test_to_set(test_input: object, expected: set[object]) -> None:
    """Test to_set function with various input types."""
    assert to_set(test_input) == expected
