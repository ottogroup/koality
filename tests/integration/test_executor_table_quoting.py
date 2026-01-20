"""Integration test for CheckExecutor to verify table name quoting when fetching data into memory."""

import duckdb
import pytest

from koality.executor import CheckExecutor
from koality.models import Config, DatabaseProvider


@pytest.mark.integration
def test_fetch_data_into_memory_quotes_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that table names are properly quoted when fetching data into memory."""
    # Minimal config without an accessor to avoid identify_database_provider during init
    cfg = Config.model_validate(
        {
            "name": "test",
            "database_setup": "",
            "database_accessor": "",
            "defaults": {"filters": {}},
            "check_bundles": [],
        },
    )

    executor = CheckExecutor(cfg)

    # Simulate that we have an accessor and a bigquery provider after initialization
    executor.config.database_accessor = "bq"
    executor.database_provider = DatabaseProvider(
        database_name="bq",
        database_oid=1,
        path="",
        comment=None,
        tags={},
        internal=False,
        type="bigquery",
        readonly=False,
        encrypted=False,
        cipher=None,
    )

    table_name = "EC0601.view_skufeed"
    data_requirements = {table_name: {"columns": {"*"}, "filters": set()}}

    captured = {"query": None}

    def fake_execute_query(
        query: str,
        duckdb_client: duckdb.DuckDBPyConnection,
        database_accessor: str,  # noqa: ARG001
        database_provider: DatabaseProvider,  # noqa: ARG001
    ) -> duckdb.DuckDBPyRelation:
        captured["query"] = query
        # Return empty relation by executing a query that yields no rows
        return duckdb_client.query("SELECT 1 WHERE FALSE")

    monkeypatch.setattr("koality.executor.execute_query", fake_execute_query)

    executor.fetch_data_into_memory(data_requirements)

    assert captured["query"] is not None
    assert f'FROM "{table_name}"' in captured["query"]
