import duckdb
import pytest

from koality.checks import CountCheck, NullRatioCheck

pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_client():
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


def test_message_no_extra_info(duckdb_client):
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        lower_threshold=1000,
        upper_threshold=9999,
    )
    check(duckdb_client)

    assert check.message == (
        "SHOP001: Metric row_count failed on 2023-01-01 for dummy_table. Value 99.0000 is not between 1000 and 9999."
    )


def test_message_date_info(duckdb_client):
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        lower_threshold=1000,
        upper_threshold=9999,
        date_info="PREDICTION_DATE = real date + 1",
    )
    check(duckdb_client)

    assert check.message == (
        "SHOP001: Metric row_count failed on 2023-01-01 "
        "(PREDICTION_DATE = real date + 1) for dummy_table. Value 99.0000 is not between 1000 and 9999."
    )


def test_message_extra_info(duckdb_client):
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        lower_threshold=1000,
        upper_threshold=9999,
        extra_info="Note: This is an awesome check.",
    )
    check(duckdb_client)

    assert check.message == (
        "SHOP001: Metric row_count failed on 2023-01-01 for dummy_table. "
        "Value 99.0000 is not between 1000 and 9999. Note: This is an awesome check."
    )


def test_message_correct_formatting():
    """
    Test checks if output still returns a useful formatted value even if
    rounding by min_precision would lead to zero.
    """
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
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        lower_threshold=1,
        upper_threshold=1,
    )
    check(conn)

    # NullRatioCheck returns 0.0 since no nulls exist
    assert check.message == (
        "SHOP001: Metric value_null_ratio failed on 2023-01-01 for dummy_table. Value 0.0000 is not between 1 and 1."
    )
