"""Utility functions for koality data quality checks."""

import contextlib
import datetime as dt
import math
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


def parse_date(date: str) -> str:
    """Parse a date string to an ISO format date.

    Supports relative terms like "today", "yesterday", or "tomorrow",
    actual dates, or relative dates with offsets like "today-2", "yesterday+1".

    Args:
        date: The date string to be parsed. Supports:
            - "today", "yesterday", "tomorrow"
            - "today+N", "today-N" (e.g., "today-2" for 2 days ago)
            - "yesterday+N", "yesterday-N" (e.g., "yesterday-2" for 3 days ago)
            - "tomorrow+N", "tomorrow-N"
            - ISO format dates like "2023-01-15"

    """
    date = str(date).lower()

    # Handle relative dates with optional offset: today, yesterday, tomorrow with +/- N
    if regex_match := re.match(r"(today|yesterday|tomorrow)([+-]\d+)?$", date):
        base = regex_match[1]
        offset_str = regex_match[2]
        offset_days = int(offset_str) if offset_str else 0

        if base == "yesterday":
            offset_days -= 1
        elif base == "tomorrow":
            offset_days += 1

        return (dt.datetime.now(tz=dt.UTC) + dt.timedelta(days=offset_days)).date().isoformat()

    # Handle ISO format dates
    return dt.datetime.fromisoformat(date).date().isoformat()


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


def substitute_variables(text: str, variables: dict[str, str]) -> str:
    """Substitute ${VAR} placeholders in text with variable values.

    Args:
        text: The text containing ${VAR} placeholders to substitute.
        variables: Dict of variable name -> value mappings.

    Returns:
        The text with all ${VAR} placeholders substituted.

    Raises:
        ValueError: If a variable is referenced but not defined.

    Example:
        >>> substitute_variables("project=${PROJECT_ID}", {"PROJECT_ID": "my-project"})
        'project=my-project'

    """
    # Find all ${VAR} patterns
    pattern = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

    def replace(match: re.Match) -> str:
        var_name = match.group(1)
        if var_name not in variables:
            msg = (
                f"Variable '${{{var_name}}}' is not defined. "
                f"Provide it via --database_setup_variable {var_name}=<value>"
            )
            raise ValueError(msg)
        return variables[var_name]

    return pattern.sub(replace, text)


def format_threshold(value: float | None) -> str:
    """Format threshold values for SQL insertion, handling infinity appropriately.

    Args:
        value: The threshold value to format (can be None, finite, or infinite).

    Returns:
        String representation suitable for SQL VALUES clause:
        - "NULL" for None
        - "'+Infinity'" for positive infinity
        - "'-Infinity'" for negative infinity
        - String representation for finite numbers

    Example:
        >>> format_threshold(None)
        'NULL'
        >>> format_threshold(float('inf'))
        "'+Infinity'"
        >>> format_threshold(float('-inf'))
        "'-Infinity'"
        >>> format_threshold(42.5)
        '42.5'

    """
    if value is None:
        return "NULL"
    if math.isinf(value):
        if value > 0:
            return "'+Infinity'"
        return "'-Infinity'"
    return str(value)
