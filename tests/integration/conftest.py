"""Pytest fixtures for integration tests."""

from collections.abc import Iterator

import duckdb
import pytest


@pytest.fixture
def duckdb_client() -> Iterator[duckdb.DuckDBPyConnection]:
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(database=":memory:")
    yield conn
    conn.close()
