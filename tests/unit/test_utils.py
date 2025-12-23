"""Unit tests for utility functions."""

import datetime as dt

import pytest

from koality.utils import parse_date, substitute_variables, to_set

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("input_date", "expected"),
    [
        ("today", dt.datetime.now(tz=dt.UTC).date().isoformat()),
        ("yesterday", (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()),
        ("tomorrow", (dt.datetime.now(tz=dt.UTC).date() + dt.timedelta(days=1)).isoformat()),
        ("today+1", (dt.datetime.now(tz=dt.UTC).date() + dt.timedelta(days=1)).isoformat()),
        ("yesterday+1", dt.datetime.now(tz=dt.UTC).date().isoformat()),
        ("tomorrow+1", (dt.datetime.now(tz=dt.UTC).date() + dt.timedelta(days=2)).isoformat()),
        ("today-2", (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=2)).isoformat()),
        ("yesterday-2", (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=3)).isoformat()),
        ("tomorrow-2", (dt.datetime.now(tz=dt.UTC).date() - dt.timedelta(days=1)).isoformat()),
        ("19901003", "1990-10-03"),
        ("1990-10-03", "1990-10-03"),
    ],
)
def test_parse_date(input_date: str, expected: str) -> None:
    """Test parse_date function with various date inputs including inline offsets."""
    assert expected == parse_date(input_date)


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


class TestSubstituteVariables:
    """Tests for substitute_variables function."""

    def test_single_variable_substitution(self) -> None:
        """Test substitution of a single variable."""
        result = substitute_variables(
            "ATTACH 'project=${PROJECT_ID}' AS bq",
            {"PROJECT_ID": "my-gcp-project"},
        )
        assert result == "ATTACH 'project=my-gcp-project' AS bq"

    def test_multiple_variable_substitution(self) -> None:
        """Test substitution of multiple variables."""
        result = substitute_variables(
            "ATTACH 'project=${PROJECT_ID}' AS ${ACCESSOR}",
            {"PROJECT_ID": "my-project", "ACCESSOR": "bq"},
        )
        assert result == "ATTACH 'project=my-project' AS bq"

    def test_no_variables_in_text(self) -> None:
        """Test text without variables is returned unchanged."""
        text = "INSTALL bigquery; LOAD bigquery;"
        result = substitute_variables(text, {"PROJECT_ID": "my-project"})
        assert result == text

    def test_empty_variables_dict_with_no_placeholders(self) -> None:
        """Test with empty variables dict and no placeholders returns text unchanged."""
        text = "ATTACH 'project=my-project' AS bq"
        result = substitute_variables(text, {})
        assert result == text

    def test_undefined_variable_raises_error(self) -> None:
        """Test that referencing undefined variable raises ValueError."""
        with pytest.raises(ValueError, match=r"Variable '\$\{PROJECT_ID\}' is not defined"):
            substitute_variables("project=${PROJECT_ID}", {})

    def test_undefined_variable_error_message_includes_hint(self) -> None:
        """Test that error message includes hint for providing variable."""
        with pytest.raises(ValueError, match=r"--database_setup_variable PROJECT_ID="):
            substitute_variables("project=${PROJECT_ID}", {})

    def test_multiline_text(self) -> None:
        """Test substitution in multiline text."""
        text = """INSTALL bigquery;
LOAD bigquery;
ATTACH 'project=${PROJECT_ID}' AS ${ACCESSOR} (TYPE bigquery);"""
        result = substitute_variables(
            text,
            {"PROJECT_ID": "my-project", "ACCESSOR": "bq"},
        )
        expected = """INSTALL bigquery;
LOAD bigquery;
ATTACH 'project=my-project' AS bq (TYPE bigquery);"""
        assert result == expected

    def test_variable_used_multiple_times(self) -> None:
        """Test that same variable can be used multiple times."""
        result = substitute_variables(
            "${VAR}-${VAR}-${VAR}",
            {"VAR": "x"},
        )
        assert result == "x-x-x"

    def test_variable_with_underscore(self) -> None:
        """Test variable names with underscores."""
        result = substitute_variables(
            "${MY_VAR_NAME}",
            {"MY_VAR_NAME": "value"},
        )
        assert result == "value"

    def test_variable_with_numbers(self) -> None:
        """Test variable names with numbers."""
        result = substitute_variables(
            "${VAR123}",
            {"VAR123": "value"},
        )
        assert result == "value"

    def test_dollar_sign_without_braces_not_substituted(self) -> None:
        """Test that $VAR (without braces) is not substituted."""
        result = substitute_variables(
            "$VAR ${VAR}",
            {"VAR": "value"},
        )
        assert result == "$VAR value"

    def test_special_characters_in_value(self) -> None:
        """Test that special characters in value are preserved."""
        result = substitute_variables(
            "project=${PROJECT_ID}",
            {"PROJECT_ID": "my-project-123_test"},
        )
        assert result == "project=my-project-123_test"
