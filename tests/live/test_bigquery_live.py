# Copyright (c) 2026 koality maintainers
"""Opt-in live tests against a real BigQuery project.

Skipped by default. Enable by setting KOALITY_BIGQUERY_LIVE=1 (see
tests/live/conftest.py and CONTRIBUTING.md for required environment
variables and authentication setup).
"""

from textwrap import dedent

import duckdb
import pytest

from koality.executor import CheckExecutor
from koality.models import Config

pytestmark = pytest.mark.bigquery_live


def test_attach_lists_tables(duckdb_bigquery_client: duckdb.DuckDBPyConnection) -> None:
    """Authenticate and attach to the configured BigQuery project."""
    result = duckdb_bigquery_client.query(
        "SELECT * FROM duckdb_databases() WHERE database_name = 'bq'",
    ).fetchone()
    assert result is not None


def test_null_ratio_check_runs_against_real_table(
    bigquery_project: str,
    bigquery_table_ref: tuple[str, str],
    bigquery_column: str,
    bigquery_secret_setup_sql: str,
) -> None:
    """Run a real koality check end-to-end against a live BigQuery table."""
    dataset, table = bigquery_table_ref
    config = Config.model_validate(
        {
            "name": "bigquery-live-smoke-test",
            "database_setup": dedent(
                f"""
                INSTALL bigquery FROM community;
                LOAD bigquery;
                {bigquery_secret_setup_sql}
                ATTACH 'project={bigquery_project}' AS bq (TYPE bigquery, READ_ONLY);
                """,
            ),
            "database_accessor": "bq",
            "defaults": {},
            "check_bundles": [
                {
                    "name": "bigquery-live-bundle",
                    "checks": [
                        {
                            "check_type": "NullRatioCheck",
                            "table": f"{dataset}.{table}",
                            "check_column": bigquery_column,
                            "upper_threshold": 1.0,
                            "monitor_only": True,
                        },
                    ],
                },
            ],
        },
    )

    results = CheckExecutor(config)()

    assert len(results) == 1
