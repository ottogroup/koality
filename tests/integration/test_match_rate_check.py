import duckdb
import pytest

from koality.checks import MatchRateCheck

pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_client() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with test data for match rate checks."""
    conn = duckdb.connect(":memory:")

    # Create purchase_order table
    conn.execute("""
        CREATE TABLE purchase_order (
            DATE DATE,
            shop_code VARCHAR,
            product_number VARCHAR
        )
    """)

    # Insert purchase data
    # - 6 rows for 2023-01-01: 1 for SHOP006 (irrelevant), 5 for SHOP001
    # - 1 row for 2022-12-31 (too early)
    # - 1 row for 2023-01-02 (too late)
    conn.execute("""
        INSERT INTO purchase_order VALUES
        ('2023-01-01', 'SHOP006', 'SHOP006-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-9999'),
        ('2022-12-31', 'SHOP001', 'SHOP001-0040'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0040')
    """)

    # Create skufeed table
    conn.execute("""
        CREATE TABLE skufeed (
            DATE DATE,
            shop_code VARCHAR,
            product_number VARCHAR
        )
    """)

    # Insert skufeed data
    conn.execute("""
        INSERT INTO skufeed VALUES
        ('2022-12-31', 'SHOP001', 'SHOP001-9999'),
        ('2023-01-02', 'SHOP001', 'SHOP001-9999'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0040'),
        ('2023-01-01', 'SHOP006', 'SHOP006-0001')
    """)

    return conn


@pytest.fixture
def duckdb_client_renamed() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with renamed columns for testing different join column names."""
    conn = duckdb.connect(":memory:")

    # Create purchase_order table with renamed column
    conn.execute("""
        CREATE TABLE purchase_order_renamed (
            DATE DATE,
            shop_code VARCHAR,
            product_number_v2 VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO purchase_order_renamed VALUES
        ('2023-01-01', 'SHOP006', 'SHOP006-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-9999'),
        ('2022-12-31', 'SHOP001', 'SHOP001-0040'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0040')
    """)

    # Create skufeed table (same as before)
    conn.execute("""
        CREATE TABLE skufeed (
            DATE DATE,
            shop_code VARCHAR,
            product_number VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO skufeed VALUES
        ('2022-12-31', 'SHOP001', 'SHOP001-9999'),
        ('2023-01-02', 'SHOP001', 'SHOP001-9999'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0040'),
        ('2023-01-01', 'SHOP006', 'SHOP006-0001')
    """)

    return conn


def test_match_rate_check(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """
    Simple check for match rate:
    - 4 / 5 product_numbers of purchases should be found
    - tests if data before / after check day are excluded
    - tests if data of other shops are excluded
    """
    check = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="purchase_order",
        right_table="skufeed",
        join_columns=["product_number"],
        check_column="product_number",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    # 4 / 5 product_numbers found (SHOP001-9999 not in skufeed on that date)
    assert result["VALUE"] == 0.8


def test_match_rate_check_join_via_2(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Tests if check also works if join is done via more than 1 column."""
    check = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="purchase_order",
        right_table="skufeed",
        join_columns=["DATE", "product_number"],
        check_column="product_number",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client)
    # 4 / 5 product_numbers found
    assert result["VALUE"] == 0.8


def test_match_rate_check_different_join_col_names(duckdb_client_renamed: duckdb.DuckDBPyConnection) -> None:
    """Tests if check also works if join is done via columns with different names."""
    check = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="purchase_order_renamed",
        right_table="skufeed",
        join_columns_left=["DATE", "product_number_v2"],
        join_columns_right=["DATE", "product_number"],
        check_column="product_number",
        shop_id_filter_column="shop_code",
        shop_id_filter_value="SHOP001",
        date_filter_column="DATE",
        date_filter_value="2023-01-01",
    )
    result = check(duckdb_client_renamed)
    # 4 / 5 product_numbers found
    assert result["VALUE"] == 0.8


@pytest.mark.parametrize(
    "day,shop",
    [
        ("2023-01-01", "SHOP999"),
        ("2022-12-30", "SHOP001"),
        ("2023-01-03", "SHOP001"),
        ("2023-01-03", "SHOP999"),
    ],
)
def test_match_rate_check_no_data(duckdb_client: duckdb.DuckDBPyConnection, day: str, shop: str) -> None:
    """Test check if there is no data for the shop / day combination."""
    check = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="purchase_order",
        right_table="skufeed",
        join_columns=["product_number"],
        check_column="product_number",
        shop_id_filter_column="shop_code",
        shop_id_filter_value=shop,
        date_filter_column="DATE",
        date_filter_value=day,
    )
    result = check(duckdb_client)
    # No data for this shop/day -> data_exists check
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == day
    assert result["SHOP_ID"] == shop
