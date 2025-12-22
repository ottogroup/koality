"""Integration tests for RelCountChangeCheck."""

import duckdb
import pytest

from koality.checks import RelCountChangeCheck

pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_client() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with test data for rel count change checks."""
    conn = duckdb.connect(":memory:")

    # Create dummy_table
    conn.execute("""
        CREATE TABLE dummy_table (
            DATE DATE,
            shop_id VARCHAR,
            product_number VARCHAR
        )
    """)

    # Insert test data:
    # - 2022-12-30: 8 rows for SHOP006 (other shop)
    # - 2022-12-31: 4 rows for SHOP001
    # - 2023-01-01: 4 rows for SHOP001
    # - 2023-01-02: 8 rows for SHOP001
    # - 2023-01-03: 6 rows for SHOP001
    conn.execute("""
        INSERT INTO dummy_table VALUES
        ('2022-12-30', 'SHOP006', 'SHOP006-0001'),
        ('2022-12-30', 'SHOP006', 'SHOP006-0002'),
        ('2022-12-30', 'SHOP006', 'SHOP006-0003'),
        ('2022-12-30', 'SHOP006', 'SHOP006-0004'),
        ('2022-12-30', 'SHOP006', 'SHOP006-0005'),
        ('2022-12-30', 'SHOP006', 'SHOP006-0006'),
        ('2022-12-30', 'SHOP006', 'SHOP006-0007'),
        ('2022-12-30', 'SHOP006', 'SHOP006-0008'),
        ('2022-12-31', 'SHOP001', 'SHOP001-0001'),
        ('2022-12-31', 'SHOP001', 'SHOP001-0002'),
        ('2022-12-31', 'SHOP001', 'SHOP001-0003'),
        ('2022-12-31', 'SHOP001', 'SHOP001-0004'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-01', 'SHOP001', 'SHOP001-0004'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0004'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0005'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0006'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0007'),
        ('2023-01-02', 'SHOP001', 'SHOP001-0008'),
        ('2023-01-03', 'SHOP001', 'SHOP001-0001'),
        ('2023-01-03', 'SHOP001', 'SHOP001-0002'),
        ('2023-01-03', 'SHOP001', 'SHOP001-0003'),
        ('2023-01-03', 'SHOP001', 'SHOP001-0004'),
        ('2023-01-03', 'SHOP001', 'SHOP001-0005'),
        ('2023-01-03', 'SHOP001', 'SHOP001-0006')
    """)

    return conn


@pytest.mark.parametrize(
    ("day", "change_rate"),
    [
        ("2023-01-02", 1.0),  # (8 - 4) / 4
        ("2023-01-03", 0.0),  # (6 - 6) / 6
    ],
)
def test_rel_count_change_check_shop_filter(
    duckdb_client: duckdb.DuckDBPyConnection,
    day: str,
    change_rate: float,
) -> None:
    """Test cases with shop restriction and different change rates for different days."""
    check = RelCountChangeCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="product_number",
        rolling_days=2,
        filters={
            "date": {"column": "DATE", "value": day, "type": "date"},
            "shop_id": {"column": "shop_id", "value": "SHOP001", "type": "identifier"},
        },
    )
    result = check(duckdb_client)
    assert result["VALUE"] == change_rate


def test_rel_count_change_check_no_history(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test case with no history data available."""
    check = RelCountChangeCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="product_number",
        rolling_days=2,
        filters={
            "date": {"column": "DATE", "value": "2022-12-31", "type": "date"},
            "shop_id": {"column": "shop_id", "value": "SHOP001", "type": "identifier"},
        },
    )
    result = check(duckdb_client)
    # No history data -> data_exists check or None value
    assert result["VALUE"] is None or result["METRIC_NAME"] == "data_exists"


def test_rel_count_change_check_no_current_data(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test case with no data for check day."""
    check = RelCountChangeCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="product_number",
        rolling_days=2,
        filters={
            "date": {"column": "DATE", "value": "2023-01-04", "type": "date"},
            "shop_id": {"column": "shop_id", "value": "SHOP001", "type": "identifier"},
        },
    )
    result = check(duckdb_client)
    assert result["METRIC_NAME"] == "data_exists"
    assert result["DATE"] == "2023-01-04"


def test_rel_count_change_check_no_shop_filter(duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test cases without shop restriction, with history leading to a decreasing row count."""
    check = RelCountChangeCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="product_number",
        rolling_days=2,
        filters={
            "date": {"column": "DATE", "value": "2022-12-31", "type": "date"},
        },
    )
    result = check(duckdb_client)
    # (4 - 8) / 8 = -0.5
    assert result["VALUE"] == -0.5
