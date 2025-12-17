"""Integration tests for column transformation checks."""

import math

import duckdb
import pytest

from koality.checks import (
    AverageCheck,
    CountCheck,
    DuplicateCheck,
    IqrOutlierCheck,
    MaxCheck,
    MinCheck,
    NullRatioCheck,
    OccurrenceCheck,
    RegexMatchCheck,
    RollingValuesInSetCheck,
    ValuesInSetCheck,
)
from koality.exceptions import KoalityError

pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_client() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")

    # Create dummy_table with test data
    conn.execute("""
        CREATE TABLE dummy_table (
            DATE DATE,
            shop_name VARCHAR,
            shop_code VARCHAR,
            product_number VARCHAR,
            num_orders FLOAT,
            assortment VARCHAR
        )
    """)

    # Insert test data - matching the original pandas DataFrame
    # 7 rows for 2023-01-01, 4 rows for 2023-01-15
    conn.execute("""
        INSERT INTO dummy_table VALUES
        ('2023-01-01', 'shop-a.example', 'SHOP001', 'SHOP001-0001', 5, 'toys'),
        ('2023-01-01', 'shop-a.example', 'SHOP001', 'SHOP001-0002', 3, 'toys'),
        ('2023-01-01', 'shop-a.example', 'SHOP001', 'SHOP001-0003', NULL, 'furniture'),
        ('2023-01-01', 'shop-a.example', 'SHOP001', 'SHOP001-0040', 0, 'clothing'),
        ('2023-01-01', 'shop-b.example', 'SHOP023', 'SHOP023-0001', 5, 'clothing'),
        ('2023-01-01', 'shop-c.example', 'SHOP002', 'SHOP002-0001', 1200, 'clothing'),
        ('2023-01-01', 'shop-d.example', 'SHOP006', 'SHOP006-0001', NULL, 'appliances'),
        ('2023-01-15', 'shop-a.example', 'SHOP001', 'SHOP001-0001', 11, 'toys'),
        ('2023-01-15', 'shop-a.example', 'SHOP001', 'SHOP001-0002', 12, 'toys'),
        ('2023-01-15', 'shop-a.example', 'SHOP001', 'SHOP001-0003', 13, 'toys'),
        ('2023-01-15', 'shop-a.example', 'SHOP001', 'SHOP001-0040', 14, 'toys')
    """)

    return conn


@pytest.fixture
def duckdb_client_iqr() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with IQR test data."""
    conn = duckdb.connect(":memory:")

    # Create dummy_table_iqr
    conn.execute("""
        CREATE TABLE dummy_table_iqr (
            BQ_PARTITIONTIME DATE,
            VALUE FLOAT
        )
    """)

    # Insert 15 days of data: days 1-10 have values 1,2,1,2..., days 11-15 have 101,102,101,102,101
    conn.execute("""
        INSERT INTO dummy_table_iqr VALUES
        ('2023-01-01', 1),
        ('2023-01-02', 2),
        ('2023-01-03', 1),
        ('2023-01-04', 2),
        ('2023-01-05', 1),
        ('2023-01-06', 2),
        ('2023-01-07', 1),
        ('2023-01-08', 2),
        ('2023-01-09', 1),
        ('2023-01-10', 2),
        ('2023-01-11', 101),
        ('2023-01-12', 102),
        ('2023-01-13', 101),
        ('2023-01-14', 102),
        ('2023-01-15', 101)
    """)

    return conn


@pytest.fixture
def duckdb_client_iqr_two_shops() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with IQR test data for two shops."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE dummy_table_iqr_two_shops (
            BQ_PARTITIONTIME DATE,
            VALUE FLOAT,
            SHOP_ID VARCHAR
        )
    """)

    # Shop 'abcd' - same as dummy_table_iqr
    conn.execute("""
        INSERT INTO dummy_table_iqr_two_shops VALUES
        ('2023-01-01', 1, 'abcd'),
        ('2023-01-02', 2, 'abcd'),
        ('2023-01-03', 1, 'abcd'),
        ('2023-01-04', 2, 'abcd'),
        ('2023-01-05', 1, 'abcd'),
        ('2023-01-06', 2, 'abcd'),
        ('2023-01-07', 1, 'abcd'),
        ('2023-01-08', 2, 'abcd'),
        ('2023-01-09', 1, 'abcd'),
        ('2023-01-10', 2, 'abcd'),
        ('2023-01-11', 101, 'abcd'),
        ('2023-01-12', 102, 'abcd'),
        ('2023-01-13', 101, 'abcd'),
        ('2023-01-14', 102, 'abcd'),
        ('2023-01-15', 101, 'abcd'),
        ('2023-01-01', 1, 'efgh'),
        ('2023-01-02', 2, 'efgh'),
        ('2023-01-03', 3, 'efgh'),
        ('2023-01-04', 4, 'efgh'),
        ('2023-01-05', 5, 'efgh'),
        ('2023-01-06', 6, 'efgh'),
        ('2023-01-07', 7, 'efgh'),
        ('2023-01-08', 8, 'efgh'),
        ('2023-01-09', 9, 'efgh'),
        ('2023-01-10', 10, 'efgh'),
        ('2023-01-11', 101, 'efgh'),
        ('2023-01-12', 102, 'efgh'),
        ('2023-01-13', 101, 'efgh'),
        ('2023-01-14', 102, 'efgh'),
        ('2023-01-15', 101, 'efgh')
    """)

    return conn


@pytest.fixture
def duckdb_client_iqr_oven() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with oven gate IQR test data."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE dummy_table_iqr_oven (
            BQ_PARTITIONTIME DATE,
            VALUE FLOAT
        )
    """)

    conn.execute("""
        INSERT INTO dummy_table_iqr_oven VALUES
        ('2024-02-01', 53),
        ('2024-02-02', 41),
        ('2024-02-04', 71),
        ('2024-02-05', 57),
        ('2024-02-06', 24),
        ('2024-02-07', 46),
        ('2024-02-08', 38),
        ('2024-02-09', 35),
        ('2024-02-11', 33),
        ('2024-02-12', 554),
        ('2024-02-13', 583),
        ('2024-02-14', 47),
        ('2024-02-15', 32)
    """)

    return conn


@pytest.fixture
def duckdb_client_iqr_latest_value_missing() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with IQR test data where latest value is NULL."""
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE dummy_table_iqr_latest_value_missing (
            BQ_PARTITIONTIME DATE,
            VALUE FLOAT
        )
    """)

    conn.execute("""
        INSERT INTO dummy_table_iqr_latest_value_missing VALUES
        ('2023-01-01', 1),
        ('2023-01-02', 2),
        ('2023-01-03', 1),
        ('2023-01-04', 2),
        ('2023-01-05', 1),
        ('2023-01-06', 2),
        ('2023-01-07', 1),
        ('2023-01-08', 2),
        ('2023-01-09', 1),
        ('2023-01-10', 2),
        ('2023-01-11', 101),
        ('2023-01-12', 102),
        ('2023-01-13', 101),
        ('2023-01-14', 102),
        ('2023-01-15', NULL)
    """)

    return conn


def test_null_ratio_check(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test NullRatioCheck returns correct ratio of NULL values."""
    check = NullRatioCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="num_orders",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    # 1 out of 4 values is NULL for SHOP001 on 2023-01-01
    assert result["VALUE"] == 0.25


def test_null_ratio_check_empty_table(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test NullRatioCheck returns data_exists failure when no data found."""
    check = NullRatioCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="num_orders",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-02",  # not in dummy_table
    )
    result = check(duckdb_client)
    # No data for this date, should return data_exists = False
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == "2023-01-02"
    assert result["TABLE"] == "dummy_table"
    assert result["SHOP_ID"] == "SHOP001"


def test_regex_match_check_all_matched(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test RegexMatchCheck returns 1.0 when all values match the pattern."""
    check = RegexMatchCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="product_number",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        regex_to_match=r"SHOP001-\d\d\d\d",  # DuckDB uses single escape
    )
    result = check(duckdb_client)
    # All 4 product numbers match for SHOP001
    assert result["VALUE"] == 1.0


def test_regex_match_check_with_unmatched(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test RegexMatchCheck returns correct ratio with partial matches."""
    check = RegexMatchCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="product_number",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        regex_to_match=r"SHOP001-000\d",  # Matches 0001, 0002, 0003 but not 0040
    )
    result = check(duckdb_client)
    # 3 out of 4 match
    assert result["VALUE"] == 0.75


def test_values_in_set_check_value_given(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Simple test case with 3/4 entries where values in value set are given."""
    check = ValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        value_set='("toys", "furniture")',
    )
    result = check(duckdb_client)
    # toys, toys, furniture, clothing -> 3/4 match
    assert result["VALUE"] == 0.75


def test_values_in_set_check_value_not_given(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test case with data for shop/day combination, but no occurrences of values in value set."""
    check = ValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        value_set='("weird non-existing value", "another weird value")',
    )
    result = check(duckdb_client)
    # No matching values -> 0.0
    assert result["VALUE"] == 0.0


@pytest.mark.parametrize(
    ("day", "shop"),
    [
        ("2023-01-01", "SHOP999"),
        ("2022-12-31", "SHOP001"),
        ("2023-01-02", "SHOP001"),
        ("2023-01-02", "SHOP999"),
    ],
)
def test_values_in_set_check_no_data(duckdb_client: duckdb.DuckDBPyConnection, day: str, shop: str) -> None:
    """Test check if there is no data for the shop / day combination."""
    check = ValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value=shop,
        date_filter_column="DATE",
        date_filter_value=day,
        value_set='("toys", "furniture")',
    )
    result = check(duckdb_client)
    # No data for this shop/day -> data_exists = False
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == day
    assert result["SHOP_ID"] == shop


def test_values_in_set_check_value_single_value(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Simple test case with single value in set."""
    check = ValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
        value_set='"toys"',
    )
    result = check(duckdb_client)
    # toys, toys, furniture, clothing -> 2/4 match
    assert result["VALUE"] == 0.5


def test_rolling_values_in_set_check_value_given_single_day(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test case with 1 day data with 2/4 entries where values in value set are given."""
    check = RollingValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-14",
        value_set='("toys")',
    )
    result = check(duckdb_client)
    # Only 2023-01-01 data is within range, 2/4 toys
    assert result["VALUE"] == 0.5


def test_rolling_values_in_set_check_value_given_2_days(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test case with 2 days data with 6/8 entries where values in value set are given."""
    check = RollingValuesInSetCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-15",
        value_set='("toys")',
    )
    result = check(duckdb_client)
    # 2023-01-01: 2/4 toys, 2023-01-15: 4/4 toys -> 6/8 total
    assert result["VALUE"] == 0.75


def test_duplicate_check_duplicates(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test duplicate check finds 1 duplicate in assortment column with 3 distinct values."""
    check = DuplicateCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    # 4 total - 3 distinct = 1 duplicate
    assert result["VALUE"] == 1


def test_duplicate_check_no_duplicates(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Simple duplicate check using assortment column for SHOP023 where no duplicates occur."""
    check = DuplicateCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP023",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    # Only 1 row for SHOP023, no duplicates
    assert result["VALUE"] == 0


def test_duplicate_check_no_data(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Simple duplicate check using assortment column for a non-existing shop_id (SHOP999)."""
    check = DuplicateCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP999",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == "2023-01-01"


def test_count_check_regular(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test simple case of counting all rows."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    # 4 rows for SHOP001 on 2023-01-01
    assert result["VALUE"] == 4


def test_count_check_distinct_column(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test simple case of counting all distinct values of a column."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="assortment",
        distinct=True,
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    # 3 distinct assortment values: toys, furniture, clothing
    assert result["VALUE"] == 3


@pytest.mark.parametrize(
    ("day", "shop"),
    [
        ("2023-01-01", "SHOP999"),
        ("2022-12-31", "SHOP001"),
        ("2023-01-02", "SHOP001"),
        ("2023-01-02", "SHOP999"),
    ],
)
def test_count_check_regular_no_data(duckdb_client: duckdb.DuckDBPyConnection, day: str, shop: str) -> None:
    """Test check if there is no data for the shop / day combination."""
    check = CountCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="*",
        shop_id_filter_column="shop_code",
        shop_id_filter_value=shop,
        date_filter_column="DATE",
        date_filter_value=day,
    )
    result = check(duckdb_client)
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == day
    assert result["SHOP_ID"] == shop


def test_average_check(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test AverageCheck computes correct mean of column values."""
    check = AverageCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="num_orders",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    assert pytest.approx(result["VALUE"], 1e-6) == (5 + 3 + 0) / 3


def test_max_check(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test MaxCheck returns the maximum value in the column."""
    check = MaxCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="num_orders",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    assert result["VALUE"] == 5


def test_min_check(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test MinCheck returns the minimum value in the column."""
    check = MinCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="num_orders",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    assert result["VALUE"] == 0


@pytest.mark.parametrize(
    ("max_or_min", "lower_threshold", "upper_threshold", "expected_value"),
    [
        ("min", 0, 2, 1),  # min occurrence is 1 (several products appear once)
        ("max", 0, 1, 2),  # max occurrence is 2 (SHOP001-0001 appears twice)
    ],
)
def test_occurrence_check(
    duckdb_client: duckdb.DuckDBPyConnection,
    max_or_min: str,
    lower_threshold: int,
    upper_threshold: int,
    expected_value: int,
) -> None:
    """Test whether any item occurs more / less often than specified."""
    check = OccurrenceCheck(
        database_accessor="",
        database_provider=None,
        max_or_min=max_or_min,
        table="dummy_table",
        check_column="product_number",
        lower_threshold=lower_threshold,
        upper_threshold=upper_threshold,
    )
    result = check(duckdb_client)
    assert result["VALUE"] == expected_value
    assert result["LOWER_THRESHOLD"] == lower_threshold
    assert result["UPPER_THRESHOLD"] == upper_threshold


def test_occurrence_check_faulty_mode() -> None:
    """Test faulty mode for occurrence check."""
    with pytest.raises(KoalityError, match="supported modes 'min' or 'max'"):
        OccurrenceCheck(
            database_accessor="",
            database_provider=None,
            max_or_min="foo",
            table="dummy_table",
            check_column="product_number",
        )


def test_iqr_outlier_check_success(duckdb_client_iqr: duckdb.DuckDBPyConnection) -> None:
    """Test IqrOutlierCheck passes when value is within IQR bounds."""
    check = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr",
        date_filter_column="BQ_PARTITIONTIME",
        date_filter_value="2023-01-15",
        interval_days=14,
        how="both",
        iqr_factor=1.5,
    )
    result = check(duckdb_client_iqr)
    assert result["VALUE"] == 101.0
    assert result["RESULT"] == "SUCCESS"
    assert result["LOWER_THRESHOLD"] == -111.875
    assert result["UPPER_THRESHOLD"] == 189.125


def test_iqr_outlier_check_two_shops_success(duckdb_client_iqr_two_shops: duckdb.DuckDBPyConnection) -> None:
    """Test IqrOutlierCheck works correctly with shop filtering for multiple shops."""
    # Test shop 'abcd'
    check = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr_two_shops",
        date_filter_column="BQ_PARTITIONTIME",
        date_filter_value="2023-01-15",
        interval_days=14,
        how="both",
        iqr_factor=1.5,
        shop_id_filter_column="SHOP_ID",
        shop_id_filter_value="abcd",
    )
    result = check(duckdb_client_iqr_two_shops)
    assert result["VALUE"] == 101.0
    assert result["RESULT"] == "SUCCESS"
    assert result["SHOP_ID"] == "abcd"

    # Test shop 'efgh'
    check2 = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr_two_shops",
        date_filter_value="2023-01-15",
        date_filter_column="BQ_PARTITIONTIME",
        interval_days=14,
        how="both",
        iqr_factor=1.5,
        shop_id_filter_column="SHOP_ID",
        shop_id_filter_value="efgh",
    )
    result2 = check2(duckdb_client_iqr_two_shops)
    assert result2["VALUE"] == 101.0
    assert result2["RESULT"] == "SUCCESS"
    assert result2["SHOP_ID"] == "efgh"


def test_iqr_outlier_check_failure(duckdb_client_iqr: duckdb.DuckDBPyConnection) -> None:
    """Test IqrOutlierCheck fails when value is outside IQR bounds."""
    check = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr",
        date_filter_value="2023-01-11",
        date_filter_column="BQ_PARTITIONTIME",
        interval_days=14,
        how="both",
        iqr_factor=1.5,
    )

    result = check(duckdb_client_iqr)
    assert result["VALUE"] == 101.0
    assert result["RESULT"] == "FAIL"


def test_iqr_outlier_check_success_because_only_lower(duckdb_client_iqr: duckdb.DuckDBPyConnection) -> None:
    """Test IqrOutlierCheck passes with how='lower' when value exceeds upper bound."""
    check = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr",
        date_filter_column="BQ_PARTITIONTIME",
        date_filter_value="2023-01-11",
        interval_days=14,
        how="lower",
        iqr_factor=1.5,
    )
    result = check(duckdb_client_iqr)
    assert result["VALUE"] == 101.0
    assert result["RESULT"] == "SUCCESS"
    assert result["UPPER_THRESHOLD"] == math.inf


@pytest.mark.parametrize(
    ("option", "match"),
    [
        ({"interval_days": 0}, "interval_days must be at least 1"),
        ({"how": "foo"}, "how must be one of"),
        ({"iqr_factor": 1.4}, "iqr_factor must be at least"),
    ],
)
def test_iqr_outlier_check_value_error(option: dict[str, object], match: str) -> None:
    """Test IqrOutlierCheck raises ValueError for invalid configuration options."""
    kwargs = {
        "database_accessor": "",
        "database_provider": None,
        "check_column": "VALUE",
        "table": "dummy_table_iqr",
        "date_filter_column": "BQ_PARTITIONTIME",
        "date_filter_value": "2023-01-11",
        "interval_days": 14,
        "how": "lower",
        "iqr_factor": 1.5,
    } | option
    with pytest.raises(KoalityError, match=match):
        IqrOutlierCheck(**kwargs)


def test_iqr_outlier_check_data_exists_error(duckdb_client_iqr_latest_value_missing: duckdb.DuckDBPyConnection) -> None:
    """Test IqrOutlierCheck returns data_exists failure when no data for date."""
    check = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr_latest_value_missing",
        date_filter_column="BQ_PARTITIONTIME",
        date_filter_value="2023-01-15",
        interval_days=14,
        how="upper",
        iqr_factor=1.5,
    )
    result = check(duckdb_client_iqr_latest_value_missing)
    assert result["METRIC_NAME"] == "data_exists"


def test_iqr_outlier_check_failure_oven_2024_02_12(duckdb_client_iqr_oven: duckdb.DuckDBPyConnection) -> None:
    """Test IqrOutlierCheck fails for oven data on 2024-02-12 with outlier value."""
    check = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr_oven",
        date_filter_column="BQ_PARTITIONTIME",
        date_filter_value="2024-02-12",
        interval_days=14,
        how="upper",
        iqr_factor=1.5,
    )
    result = check(duckdb_client_iqr_oven)
    assert result["VALUE"] == 554.0
    assert result["RESULT"] == "FAIL"


def test_iqr_outlier_check_failure_oven_2024_02_13(duckdb_client_iqr_oven: duckdb.DuckDBPyConnection) -> None:
    """Test IqrOutlierCheck fails for oven data on 2024-02-13 with outlier value."""
    check = IqrOutlierCheck(
        database_accessor="",
        database_provider=None,
        check_column="VALUE",
        table="dummy_table_iqr_oven",
        date_filter_column="BQ_PARTITIONTIME",
        date_filter_value="2024-02-13",
        interval_days=14,
        how="upper",
        iqr_factor=1.5,
    )
    result = check(duckdb_client_iqr_oven)
    assert result["VALUE"] == 583.0
    assert result["RESULT"] == "FAIL"
