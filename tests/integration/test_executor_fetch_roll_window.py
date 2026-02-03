"""Integration test for executor fetch with rolling windows."""

import duckdb

from koality.checks import RelCountChangeCheck
from koality.executor import CheckExecutor
from koality.models import Config, _CheckBundle, _GlobalDefaults


def test_fetch_data_into_memory_respects_rolling_window() -> None:
    """Ensure fetch_data_into_memory restricts rows to the rolling date window."""
    conn = duckdb.connect(":memory:")

    # Create dummy_table with DATE and shop_id
    conn.execute(
        """
        CREATE TABLE dummy_table (
            DATE DATE,
            shop_id VARCHAR,
            product_number VARCHAR
        )
        """,
    )

    # Insert rows spanning multiple dates and shops
    conn.execute(
        """
        INSERT INTO dummy_table VALUES
        ('2023-01-01', 'SHOP001', 'P1'),
        ('2023-01-02', 'SHOP001', 'P2'),
        ('2023-01-03', 'SHOP001', 'P3'),
        ('2023-01-01', 'SHOP002', 'P4'),
        ('2023-01-04', 'SHOP001', 'P5')
        """,
    )

    # Create a RelCountChangeCheck targeting 2023-01-03 with rolling_days=2
    check = RelCountChangeCheck(
        database_accessor="",
        database_provider=None,
        table="dummy_table",
        check_column="product_number",
        rolling_days=2,
        filters={
            "date": {"column": "DATE", "value": "2023-01-03", "type": "date"},
            "shop_id": {"column": "shop_id", "value": "SHOP001", "type": "identifier"},
        },
    )

    # Minimal Config for executor - database_setup empty and accessor set to empty string
    cfg = Config(
        name="test",
        database_setup="",
        database_accessor="",
        defaults=_GlobalDefaults(),
        check_bundles=[_CheckBundle(name="bundle", checks=[])],
    )

    executor = CheckExecutor(cfg, duckdb_client=conn)
    # Add our check directly
    executor.checks.append(check)

    # Get data requirements and fetch into memory
    data_reqs = executor.get_data_requirements()
    executor.fetch_data_into_memory(data_reqs)

    # Query the in-memory table
    # Normalize DATE to ISO string for comparison
    res = [
        (row[0].isoformat(), row[1], row[2])
        for row in conn.execute(
            'SELECT DATE, shop_id, product_number FROM "dummy_table" ORDER BY DATE, product_number',
        ).fetchall()
    ]

    expected = [
        ("2023-01-01", "SHOP001", "P1"),
        ("2023-01-02", "SHOP001", "P2"),
        ("2023-01-03", "SHOP001", "P3"),
    ]

    assert res == expected
