from typing import Iterator

import duckdb
import pytest


@pytest.fixture
def duckdb_client() -> Iterator[duckdb.DuckDBPyConnection]:
    import duckdb

    conn = duckdb.connect(database=":memory:")
    yield conn
    conn.close()
