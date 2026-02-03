"""Integration test for underscore aliasing of nested column names."""

import duckdb
import pytest

from koality.checks import MatchRateCheck, NullRatioCheck

pytestmark = pytest.mark.integration


def test_underscore_aliasing_with_struct_columns() -> None:
    """Test that nested/struct columns are aliased with underscores when loaded into cache.

    This test verifies the complete flow:
    1. Data with struct columns (value.shopId, value.productId) gets loaded
    2. Columns are aliased as value_shopId, value_productId in DuckDB
    3. Checks reference dotted notation (value.shopId) in config
    4. Check queries automatically convert to underscore notation (value_shopId)
    """
    conn = duckdb.connect(":memory:")

    # Create source table with actual STRUCT columns
    conn.execute("""
        CREATE TABLE source_data (
            BQ_PARTITIONTIME DATE,
            value STRUCT(shopId VARCHAR, productId VARCHAR, price DOUBLE)
        )
    """)

    conn.execute("""
        INSERT INTO source_data VALUES
        ('2023-01-01', {'shopId': 'EC1705', 'productId': 'PROD-001', 'price': 19.99}),
        ('2023-01-01', {'shopId': 'EC1705', 'productId': 'PROD-002', 'price': 29.99}),
        ('2023-01-01', {'shopId': 'EC1705', 'productId': NULL, 'price': 39.99}),
        ('2023-01-01', {'shopId': 'EC1706', 'productId': 'PROD-004', 'price': 49.99})
    """)

    # Simulate data loading with flattening (as executor would do)
    # This creates table with underscore-aliased column names
    conn.execute("""
        CREATE TABLE loaded_data AS
        SELECT
            BQ_PARTITIONTIME,
            value.shopId AS value_shopId,
            value.productId AS value_productId,
            value.price AS value_price
        FROM source_data
    """)

    # Create check using dotted notation (as in config) AND non-empty database_accessor
    check = NullRatioCheck(
        database_accessor="bigquery.project.dataset",  # Non-empty to trigger underscore conversion
        database_provider=None,
        table="loaded_data",
        check_column="value.productId",  # Config uses dotted notation
        filters={
            "partition_date": {"column": "BQ_PARTITIONTIME", "value": "2023-01-01", "type": "date"},
            "shop_id": {"column": "value.shopId", "value": "EC1705", "type": "identifier"},  # Filter also uses dotted
        },
    )

    result = check(conn)
    # 1 NULL out of 3 rows (EC1705 only) = 0.333
    assert abs(result["VALUE"] - 0.333) < 0.01


def test_underscore_aliasing_matchrate_with_mixed_columns() -> None:
    """Test MatchRateCheck with mix of regular and underscore-aliased columns.

    This tests a realistic scenario where:
    - Left table has regular flat columns
    - Right table has underscore-aliased struct columns
    - Config references right table columns with dots
    """
    conn = duckdb.connect(":memory:")

    # Left table: regular flat columns (simulating already-processed data)
    conn.execute("""
        CREATE TABLE orders (
            order_date DATE,
            shop_id VARCHAR,
            product_id VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO orders VALUES
        ('2023-01-01', 'EC1705', 'PROD-001'),
        ('2023-01-01', 'EC1705', 'PROD-002'),
        ('2023-01-01', 'EC1705', 'PROD-003'),
        ('2023-01-01', 'EC1705', 'PROD-999')
    """)

    # Right table: underscore-aliased struct columns (simulating loaded BigQuery data)
    conn.execute("""
        CREATE TABLE products (
            date DATE,
            value_shopId VARCHAR,
            value_productId VARCHAR,
            value_inStock BOOLEAN
        )
    """)

    conn.execute("""
        INSERT INTO products VALUES
        ('2023-01-01', 'EC1705', 'PROD-001', TRUE),
        ('2023-01-01', 'EC1705', 'PROD-002', TRUE),
        ('2023-01-01', 'EC1705', 'PROD-003', FALSE)
    """)

    # Create MatchRateCheck with dotted notation for right table AND non-empty database_accessor
    check = MatchRateCheck(
        database_accessor="bigquery.project.dataset",  # Non-empty to trigger underscore conversion
        database_provider=None,
        left_table="orders",
        right_table="products",
        join_columns_left=["order_date", "shop_id", "product_id"],
        join_columns_right=["date", "value.shopId", "value.productId"],  # Dotted notation
        check_column="product_id",
        filters_left={
            "date": {"column": "order_date", "value": "2023-01-01", "type": "date"},
            "shop": {"column": "shop_id", "value": "EC1705", "type": "identifier"},
        },
        filters_right={
            "date": {"column": "date", "value": "2023-01-01", "type": "date"},
            "shop": {"column": "value.shopId", "value": "EC1705", "type": "identifier"},  # Dotted in filter
        },
    )

    result = check(conn)
    # 3 out of 4 products found (PROD-999 missing)
    assert result["VALUE"] == 0.75


def test_underscore_aliasing_matchrate_with_dotted_left_columns() -> None:
    """Test MatchRateCheck when BOTH left and right columns have dotted names.

    This tests the scenario from the user's config where left table also has
    struct columns (like orderLine.skuId) that get flattened to orderLine_skuId.
    """
    conn = duckdb.connect(":memory:")

    # Left table with underscore-aliased columns
    conn.execute("""
        CREATE TABLE purchases (
            BQ_PARTITIONTIME DATE,
            shopId VARCHAR,
            orderLine_skuId VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO purchases VALUES
        ('2023-01-01', 'EC1701', 'SKU-001'),
        ('2023-01-01', 'EC1701', 'SKU-002'),
        ('2023-01-01', 'EC1701', 'SKU-003'),
        ('2023-01-01', 'EC1701', 'SKU-999')
    """)

    # Right table with underscore-aliased columns
    conn.execute("""
        CREATE TABLE skufeed (
            BQ_PARTITIONTIME DATE,
            value_shopId VARCHAR,
            value_clickstreamskuId VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO skufeed VALUES
        ('2023-01-01', 'EC1701', 'SKU-001'),
        ('2023-01-01', 'EC1701', 'SKU-002'),
        ('2023-01-01', 'EC1701', 'SKU-003')
    """)

    # Create MatchRateCheck with dotted notation in BOTH left and right columns
    check = MatchRateCheck(
        database_accessor="bigquery.project.dataset",
        database_provider=None,
        left_table="purchases",
        right_table="skufeed",
        join_columns_left=["BQ_PARTITIONTIME", "shopId", "orderLine.skuId"],  # Left has dot!
        join_columns_right=["BQ_PARTITIONTIME", "value.shopId", "value.clickstreamskuId"],
        check_column="orderLine.skuId",
        filters_left={
            "date": {"column": "BQ_PARTITIONTIME", "value": "2023-01-01", "type": "date"},
            "shop": {"column": "shopId", "value": "EC1701", "type": "identifier"},
        },
        filters_right={
            "date": {"column": "BQ_PARTITIONTIME", "value": "2023-01-01", "type": "date"},
            "shop": {"column": "value.shopId", "value": "EC1701", "type": "identifier"},
        },
    )

    result = check(conn)
    # 3 out of 4 SKUs found (SKU-999 not in skufeed)
    assert result["VALUE"] == 0.75


def test_underscore_aliasing_with_multiple_nesting_levels() -> None:
    """Test underscore aliasing with deeply nested column names.

    Verifies that multi-level nesting like 'data.value.shopId' becomes 'data_value_shopId'.
    """
    conn = duckdb.connect(":memory:")

    # Create table with multi-level underscore-aliased columns
    conn.execute("""
        CREATE TABLE nested_data (
            date DATE,
            data_value_shopId VARCHAR,
            data_value_price DOUBLE,
            data_meta_source VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO nested_data VALUES
        ('2023-01-01', 'EC1705', 19.99, 'API'),
        ('2023-01-01', 'EC1705', NULL, 'API'),
        ('2023-01-01', 'EC1706', 29.99, 'MANUAL')
    """)

    # Check using multi-level dotted notation AND non-empty database_accessor
    check = NullRatioCheck(
        database_accessor="bigquery.project.dataset",  # Non-empty to trigger underscore conversion
        database_provider=None,
        table="nested_data",
        check_column="data.value.price",  # Three-level nesting
        filters={
            "date": {"column": "date", "value": "2023-01-01", "type": "date"},
            "shop": {"column": "data.value.shopId", "value": "EC1705", "type": "identifier"},  # Multi-level in filter
        },
    )

    result = check(conn)
    # 1 NULL out of 2 rows (EC1705 only)
    assert result["VALUE"] == 0.5
