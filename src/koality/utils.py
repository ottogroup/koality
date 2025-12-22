"""Utility functions for koality data quality checks."""

import contextlib
import datetime as dt
import re
from ast import literal_eval
from collections.abc import Iterable
from logging import getLogger

import duckdb

from koality.exceptions import DatabaseError
from koality.models import DatabaseProvider

log = getLogger(__name__)


def identify_database_provider(
    duckdb_client: duckdb.DuckDBPyConnection,
    database_accessor: str,
) -> DatabaseProvider:
    """Identify the database provider type from a DuckDB database accessor.

    Args:
        duckdb_client: DuckDB client connection.
        database_accessor: The name of the attached database.

    Returns:
        DatabaseProvider with type information (e.g., 'bigquery', 'postgres').

    Raises:
        DatabaseError: If the database accessor is not found in DuckDB databases.

    """
    # Check if the database accessor is of type bigquery
    result = duckdb_client.query(f"SELECT * FROM duckdb_databases() WHERE database_name = '{database_accessor}'")  # noqa: S608
    column_names = [desc[0] for desc in result.description]
    first = result.fetchone()
    if first is None:
        msg = f"Database accessor '{database_accessor}' not found in duckdb databases."
        raise DatabaseError(msg)
    return DatabaseProvider(**dict(zip(column_names, first, strict=False)))


def execute_query(
    query: str,
    duckdb_client: duckdb.DuckDBPyConnection,
    database_provider: DatabaseProvider | None,
) -> duckdb.DuckDBPyRelation:
    """Execute a query, using bigquery_query() if the accessor is a BigQuery database.

    This handles the limitation where BigQuery's Storage Read API cannot read views.
    When a BigQuery accessor is detected, the query is wrapped in bigquery_query()
    which uses the Jobs API instead.

    Note: bigquery_query() only works for SELECT queries. Write operations
    (INSERT, CREATE, UPDATE, DELETE) use standard DuckDB execution with the accessor prefix.
    """
    if database_provider:
        if database_provider.type == "bigquery":
            # Check if this is a write operation
            query_upper = query.strip().upper()
            is_write_operation = query_upper.startswith(("INSERT", "CREATE", "UPDATE", "DELETE", "DROP", "ALTER"))

            # path -> google cloud project
            project = database_provider.path

            # Use dollar-quoting to avoid escaping issues with single quotes in the query
            if is_write_operation:
                # Use bigquery_execute for write operations
                wrapped_query = f"CALL bigquery_execute('{project}', $bq${query}$bq$)"
            else:
                # Use bigquery_query for read operations (supports views)
                wrapped_query = f"SELECT * FROM bigquery_query('{project}', $bq${query}$bq$)"  # noqa: S608

            return duckdb_client.query(wrapped_query)
        log.info("Database is of type '%s'. Using standard query execution.", database_provider.type)

    return duckdb_client.query(query)


def parse_date(date: str, offset_days: int = 0) -> str:
    """Parse a date string to an ISO format date.

    Supports relative terms like "today", "yesterday", or "tomorrow",
    actual dates, or relative dates like "today-2".

    Args:
        date: The date string to be parsed.
        offset_days: The number of days to be added/substracted.

    """
    date = str(date).lower()

    if date == "today":
        return (dt.datetime.now(tz=dt.UTC) + dt.timedelta(days=offset_days)).date().isoformat()

    if date == "yesterday":
        offset_days -= 1
        return (dt.datetime.now(tz=dt.UTC) + dt.timedelta(days=offset_days)).date().isoformat()

    if date == "tomorrow":
        offset_days += 1
        return (dt.datetime.now(tz=dt.UTC) + dt.timedelta(days=offset_days)).date().isoformat()

    if regex_match := re.search(r"today([+-][0-9]+)", date):
        offset_days += int(regex_match[1])
        return (dt.datetime.now(tz=dt.UTC) + dt.timedelta(days=offset_days)).date().isoformat()

    return (dt.datetime.fromisoformat(date) + dt.timedelta(days=offset_days)).date().isoformat()


def parse_arg(arg: str) -> str | int | bool:
    """Parse a string argument into its appropriate Python type.

    Converts 'true'/'false' to booleans and numeric strings to int/float.
    """
    if arg.lower() == "false":
        return False
    if arg.lower() == "true":
        return True

    if re.fullmatch(r"\d+(\.\d+)?", arg):  # if is int or float
        return literal_eval(arg)

    return arg


def to_set(value: object) -> set[object]:
    """Convert the input value to a set.

    The special case of one single string is also covered. Duplicates are also
    removed and for deterministic behavior, the values are sorted.

    Convert input as follows:
    - 1 -> {1}
    - True -> {True}
    - "toys" / '"toys"' -> {"toys"}
    - ("toys") / '("toys")' -> {"toys"}
    - ("toys", "shirt") / '("toys", "shirt")' -> {"shirt", "toys"}
    - ["toys"] -> {"toys"}
    - {"toys"} -> {"toys"}

    """
    with contextlib.suppress(ValueError):
        value = literal_eval(value)
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        return {value}
    if isinstance(value, set):
        return value
    return set(value)
