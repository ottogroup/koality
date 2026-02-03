"""Integration tests for MatchRateCheck."""

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
    """Test match rate check with shop and date filtering.

    Validates:
    - 4 / 5 product_numbers of purchases should be found
    - Data before / after check day are excluded
    - Data of other shops are excluded
    """
    check = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="purchase_order",
        right_table="skufeed",
        join_columns=["product_number"],
        check_column="product_number",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
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
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
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
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
    )
    result = check(duckdb_client_renamed)
    # 4 / 5 product_numbers found
    assert result["VALUE"] == 0.8


def test_match_rate_check_dotted_column_names(duckdb_client_dotted_columns: duckdb.DuckDBPyConnection) -> None:
    """Test match rate check with dotted column names in join_columns_right (like 'value.shopId').

    This tests the fix where dotted column names in join_columns_right are converted to
    underscore-aliased names (value.shopId → value_shopId) to match the column names
    created when loading data from external sources. The test simulates how BigQuery struct
    columns are aliased - in the actual table they're stored as 'value_shopId', but in the
    config they're referenced as 'value.shopId'.

    Note: Uses a non-empty database_accessor to indicate data was loaded/flattened.
    """
    check = MatchRateCheck(
        database_accessor="bigquery.project.dataset",  # Non-empty to trigger underscore conversion
        database_provider=None,
        left_table="tracking_purchase",
        right_table="skufeed",
        join_columns_left=["BQ_PARTITIONTIME", "shopId", "product_number"],
        join_columns_right=["BQ_PARTITIONTIME", "value.shopId", "product_number"],
        check_column="product_number",
        filters={
            "partition_date": {"column": "BQ_PARTITIONTIME", "value": "2023-01-01", "type": "date"},
        },
        filters_left={
            "shop_id": {"column": "shopId", "value": "EC1705", "type": "identifier"},
        },
        filters_right={
            "shop_id": {"column": "value.shopId", "value": "EC1705", "type": "identifier"},
        },
    )
    result = check(duckdb_client_dotted_columns)
    # 3 / 4 product_numbers found (PROD-999 not in skufeed)
    assert result["VALUE"] == 0.75


def test_match_rate_check_with_struct_columns(duckdb_client_with_struct: duckdb.DuckDBPyConnection) -> None:
    """Test match rate check with dotted column names that get aliased with underscores when cached.

    This simulates the real BigQuery → DuckDB scenario: In BigQuery config, struct columns
    are referenced as 'value.shopId', but when the data is loaded into DuckDB cache,
    these columns are aliased with underscores ('value_shopId'). The fix ensures that:
    1. The SELECT DISTINCT statement uses the underscore-aliased name ('value_shopId' not 'value.shopId')
    2. The JOIN conditions use the underscore-aliased name ('righty.value_shopId')
    3. The WHERE clauses use the underscore-aliased name (via strip_dotted_columns)
    """
    check = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="tracking_purchase",
        right_table="skufeed",
        join_columns_left=["BQ_PARTITIONTIME", "shopId", "product_number"],
        join_columns_right=["BQ_PARTITIONTIME", "value.shopId", "product_number"],  # Config uses dotted notation
        check_column="product_number",
        filters={
            "partition_date": {"column": "BQ_PARTITIONTIME", "value": "2023-01-01", "type": "date"},
        },
        filters_left={
            "shop_id": {"column": "shopId", "value": "EC1705", "type": "identifier"},
        },
        filters_right={
            "shop_id": {"column": "value.shopId", "value": "EC1705", "type": "identifier"},
        },
    )
    result = check(duckdb_client_with_struct)
    # 3 / 4 product_numbers found (PROD-999 not in skufeed)
    assert result["VALUE"] == 0.75


@pytest.mark.parametrize(
    ("day", "shop"),
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
        filters={
            "shop_id": {"column": "shop_code", "value": shop, "type": "identifier"},
            "date": {"column": "DATE", "value": day, "type": "date"},
        },
    )
    result = check(duckdb_client)
    # No data for this shop/day -> data_exists check
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == day
    assert result["IDENTIFIER"] == f"shop_code={shop}"


@pytest.fixture
def duckdb_client_dotted_columns() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with underscore-aliased column names.

    This simulates data loaded from external sources where struct columns like 'value.shopId'
    are aliased to 'value_shopId' using deterministic underscore replacement.
    """
    conn = duckdb.connect(":memory:")

    # Create tracking table
    conn.execute("""
        CREATE TABLE tracking_purchase (
            BQ_PARTITIONTIME DATE,
            shopId VARCHAR,
            product_number VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO tracking_purchase VALUES
        ('2023-01-01', 'EC1705', 'PROD-001'),
        ('2023-01-01', 'EC1705', 'PROD-002'),
        ('2023-01-01', 'EC1705', 'PROD-003'),
        ('2023-01-01', 'EC1705', 'PROD-999')
    """)

    # Create skufeed table with underscore-aliased struct columns
    # In config these are referenced as 'value.shopId', in DuckDB they're 'value_shopId'
    conn.execute("""
        CREATE TABLE skufeed (
            BQ_PARTITIONTIME DATE,
            value_shopId VARCHAR,
            clickstreamSkuId VARCHAR,
            product_number VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO skufeed VALUES
        ('2023-01-01', 'EC1705', 'SKU-001', 'PROD-001'),
        ('2023-01-01', 'EC1705', 'SKU-002', 'PROD-002'),
        ('2023-01-01', 'EC1705', 'SKU-003', 'PROD-003')
    """)

    return conn


@pytest.fixture
def duckdb_client_with_struct() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with actual STRUCT columns.

    This fixture creates tables with real nested STRUCT columns (like BigQuery has)
    to test that dotted notation in config (value.shopId) properly accesses nested fields.
    """
    conn = duckdb.connect(":memory:")

    # Create tracking table with regular columns
    conn.execute("""
        CREATE TABLE tracking_purchase (
            BQ_PARTITIONTIME DATE,
            shopId VARCHAR,
            product_number VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO tracking_purchase VALUES
        ('2023-01-01', 'EC1705', 'PROD-001'),
        ('2023-01-01', 'EC1705', 'PROD-002'),
        ('2023-01-01', 'EC1705', 'PROD-003'),
        ('2023-01-01', 'EC1705', 'PROD-999')
    """)

    # Create skufeed table with actual STRUCT columns (nested data)
    # This represents raw BigQuery data with struct types
    conn.execute("""
        CREATE TABLE skufeed (
            BQ_PARTITIONTIME DATE,
            value STRUCT(shopId VARCHAR, clickstreamSkuId VARCHAR),
            product_number VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO skufeed VALUES
        ('2023-01-01', {'shopId': 'EC1705', 'clickstreamSkuId': 'SKU-001'}, 'PROD-001'),
        ('2023-01-01', {'shopId': 'EC1705', 'clickstreamSkuId': 'SKU-002'}, 'PROD-002'),
        ('2023-01-01', {'shopId': 'EC1705', 'clickstreamSkuId': 'SKU-003'}, 'PROD-003')
    """)

    return conn


@pytest.fixture
def duckdb_client_one_table_empty() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with one empty table."""
    conn = duckdb.connect(":memory:")

    # Create purchase_order table
    conn.execute("""
        CREATE TABLE purchase_order (
            DATE DATE,
            shop_code VARCHAR,
            product_number VARCHAR
        )
    """)

    # Create skufeed table
    conn.execute("""
        CREATE TABLE skufeed (
            DATE DATE,
            shop_code VARCHAR,
            product_number VARCHAR
        )
    """)

    return conn


@pytest.mark.parametrize(
    ("left_empty"),
    [
        (True),
        (False),
    ],
)
def test_match_rate_check_one_table_empty(
    duckdb_client_one_table_empty: duckdb.DuckDBPyConnection,
    *,
    left_empty: bool,
) -> None:
    """Test check if one of the tables is empty."""
    conn = duckdb_client_one_table_empty
    if left_empty:
        # Insert skufeed data
        conn.execute("""
            INSERT INTO skufeed VALUES
            ('2023-01-01', 'SHOP001', 'SHOP001-0001')
        """)
    else:
        # Insert purchase data
        conn.execute("""
            INSERT INTO purchase_order VALUES
            ('2023-01-01', 'SHOP001', 'SHOP001-0001')
        """)

    check = MatchRateCheck(
        database_accessor="",
        database_provider=None,
        left_table="purchase_order",
        right_table="skufeed",
        join_columns=["product_number"],
        check_column="product_number",
        filters={
            "shop_id": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
    )
    result = check(conn)
    # No data for this shop/day -> data_exists check
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == "2023-01-01"
    assert result["IDENTIFIER"] == "shop_code=SHOP001"
