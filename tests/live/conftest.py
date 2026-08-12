# Copyright (c) 2026 koality maintainers
"""Fixtures for opt-in live BigQuery tests.

These tests hit a real BigQuery project and are skipped unless explicitly
enabled. Authenticate via Application Default Credentials (e.g. Workload
Identity Federation) before running them - see CONTRIBUTING.md.
"""

import os
from collections.abc import Iterator

import duckdb
import pytest

ENABLE_ENV = "KOALITY_BIGQUERY_LIVE"
PROJECT_ENV = "KOALITY_BIGQUERY_PROJECT"
DATASET_ENV = "KOALITY_BIGQUERY_DATASET"
TABLE_ENV = "KOALITY_BIGQUERY_TABLE"
COLUMN_ENV = "KOALITY_BIGQUERY_COLUMN"
ACCESS_TOKEN_ENV = "KOALITY_BIGQUERY_ACCESS_TOKEN"  # noqa: S105 -- env var name, not a credential


@pytest.fixture(autouse=True)
def _require_live_bigquery() -> None:
    """Skip all tests in this package unless live BigQuery testing is enabled."""
    if not os.environ.get(ENABLE_ENV):
        pytest.skip(f"Set {ENABLE_ENV}=1 to run live BigQuery tests (requires GCP credentials).")


@pytest.fixture
def bigquery_project() -> str:
    """Return the GCP project ID to attach, provided by the caller."""
    project = os.environ.get(PROJECT_ENV)
    if not project:
        pytest.skip(f"Set {PROJECT_ENV} to the GCP project ID to test against.")
    return project


@pytest.fixture
def bigquery_table_ref() -> tuple[str, str]:
    """Return the (dataset, table) pair to run checks against, provided by the caller."""
    dataset = os.environ.get(DATASET_ENV)
    table = os.environ.get(TABLE_ENV)
    if not dataset or not table:
        pytest.skip(f"Set {DATASET_ENV} and {TABLE_ENV} to a fixture table to test against.")
    return dataset, table


@pytest.fixture
def bigquery_column() -> str:
    """Return a column on the fixture table, provided by the caller."""
    column = os.environ.get(COLUMN_ENV)
    if not column:
        pytest.skip(f"Set {COLUMN_ENV} to a column on the fixture table.")
    return column


@pytest.fixture
def bigquery_secret_setup_sql(bigquery_project: str) -> str:
    """Return SQL registering a DuckDB secret from a pre-fetched access token, if provided.

    In CI, feeding a plain OAuth2 access token via a DuckDB secret avoids the
    BigQuery Storage Read API's parallel gRPC streams each independently
    re-deriving credentials through GitHub's short-lived OIDC token endpoint,
    which otherwise exhausts the runner's OIDC minting quota on longer
    queries. Falls back to ADC (e.g. local `gcloud auth application-default
    login`) when no token is provided; returns an empty string in that case.
    """
    token = os.environ.get(ACCESS_TOKEN_ENV)
    if not token:
        return ""
    return f"""
        CREATE SECRET bigquery_token (
            TYPE bigquery,
            SCOPE 'bq://{bigquery_project}',
            ACCESS_TOKEN '{token}'
        );
        """


@pytest.fixture
def duckdb_bigquery_client(
    bigquery_project: str,
    bigquery_secret_setup_sql: str,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Return a DuckDB connection with the bigquery extension loaded and attached."""
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL bigquery FROM community")
    conn.execute("LOAD bigquery")
    if bigquery_secret_setup_sql:
        conn.execute(bigquery_secret_setup_sql)
    conn.execute(f"ATTACH 'project={bigquery_project}' AS bq (TYPE bigquery, READ_ONLY)")
    yield conn
    conn.close()
