"""Integration tests for basic check classes."""

import duckdb
import pytest

from koality.checks import CountCheck, NullRatioCheck

pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_client() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")
    # Create a test table with sample data
    conn.execute("""
        CREATE TABLE dummy_table (
            shop_code VARCHAR,
            DATE DATE,
            value FLOAT
        )
    """)
    # Insert test data: 99 rows for SHOP001 on 2023-01-01
    conn.execute("""
        INSERT INTO dummy_table
        SELECT 'SHOP001', '2023-01-01'::DATE, random()
        FROM range(99)
    """)
    return conn


def test_message_no_extra_info(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test that failure message is correctly formatted without extra info."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=1000,
        upper_threshold=9999,
    )
    check(duckdb_client)

    assert check.message == (
        "shop_code=SHOP001: Metric row_count failed on 2023-01-01 for dummy_table. "
        "Value 99.0000 is not between 1000 and 9999."
    )


def test_message_date_info(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test that failure message includes date_info when provided."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=1000,
        upper_threshold=9999,
        date_info="PREDICTION_DATE = real date + 1",
    )
    check(duckdb_client)

    assert check.message == (
        "shop_code=SHOP001: Metric row_count failed on 2023-01-01 "
        "(PREDICTION_DATE = real date + 1) for dummy_table. Value 99.0000 is not between 1000 and 9999."
    )


def test_message_extra_info(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test that failure message includes extra_info when provided."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=1000,
        upper_threshold=9999,
        extra_info="Note: This is an awesome check.",
    )
    check(duckdb_client)

    assert check.message == (
        "shop_code=SHOP001: Metric row_count failed on 2023-01-01 for dummy_table. "
        "Value 99.0000 is not between 1000 and 9999. Note: This is an awesome check."
    )


def test_message_correct_formatting() -> None:
    """Test that output returns a useful formatted value even when rounding leads to zero."""
    # Create a separate duckdb client with specific test data for this test
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE dummy_table (
            shop_code VARCHAR,
            DATE DATE,
            value FLOAT
        )
    """)
    # Insert a single row to get count of 1, but we need the check value to be 0.0000123
    # For CountCheck, the value is the count, so we need a different approach
    # Let's use a NullRatioCheck or similar for this test case
    conn.execute("INSERT INTO dummy_table VALUES ('SHOP001', '2023-01-01', 0.0000123)")

    check = NullRatioCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="value",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=1,
        upper_threshold=1,
    )
    check(conn)

    # NullRatioCheck returns 0.0 since no nulls exist
    assert check.message == (
        "shop_code=SHOP001: Metric value_null_ratio failed on 2023-01-01 for dummy_table. "
        "Value 0.0000 is not between 1 and 1."
    )


def test_identifier_format_filter_name(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test identifier_format='filter_name' uses filter name as column and value as-is."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        identifier_format="filter_name",
    )
    result = check(duckdb_client)

    # With filter_name format: column is SHOP_ID, value is SHOP001
    assert "SHOP_ID" in result
    assert result["SHOP_ID"] == "SHOP001"
    assert check.identifier == "SHOP001"
    assert check.identifier_column == "SHOP_ID"


def test_identifier_format_column_name(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test identifier_format='column_name' uses database column name as column header."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        identifier_format="column_name",
    )
    result = check(duckdb_client)

    # With column_name format: column is SHOP_CODE, value is SHOP001
    assert "SHOP_CODE" in result
    assert result["SHOP_CODE"] == "SHOP001"
    assert check.identifier == "SHOP001"
    assert check.identifier_column == "SHOP_CODE"


def test_identifier_format_identifier_default(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test default identifier_format='identifier' uses column=value format."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        # identifier_format defaults to "identifier"
    )
    result = check(duckdb_client)

    # With identifier format: column is IDENTIFIER, value is shop_code=SHOP001
    assert "IDENTIFIER" in result
    assert result["IDENTIFIER"] == "shop_code=SHOP001"
    assert check.identifier == "shop_code=SHOP001"
    assert check.identifier_column == "IDENTIFIER"


def test_missing_table_maps_to_table_exists() -> None:
    """Querying a non-existent table should produce a table_exists failure."""
    conn = duckdb.connect(":memory:")

    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="missing_table",
        check_column="*",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
    )

    result = check.check(conn)

    assert result["METRIC_NAME"] == "table_exists"
    assert result["RESULT"] == "FAIL"
    assert result["TABLE"] == "missing_table"
