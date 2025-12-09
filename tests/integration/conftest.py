import pytest


@pytest.fixture
def duckdb_client():
    import duckdb

    conn = duckdb.connect(database=":memory:")
    yield conn
    conn.close()
