"""Utils for big expectations"""

from collections import defaultdict
from logging import getLogger
import re
from ast import literal_eval
from collections.abc import Iterable
from typing import Any, Union
import datetime as dt

import duckdb

from koality.models import DatabaseProvider

log = getLogger(__name__)


def identify_database_provider(
    duckdb_client: duckdb.DuckDBPyConnection,
    database_accessor: str,
) -> DatabaseProvider:
    # Check if the database accessor is of type bigquery
    result = duckdb_client.query(
        f"""
        SELECT * 
        FROM duckdb_databases() 
        WHERE database_name = '{database_accessor}'
        """
    )
    column_names = [desc[0] for desc in result.description]
    first = result.fetchone()
    if first is None:
        raise KeyError(f"Database accessor '{database_accessor}' not found in duckdb databases.")
    return DatabaseProvider(**dict(zip(column_names, first)))


def execute_query(
    query: str,
    duckdb_client: duckdb.DuckDBPyConnection,
    database_provider: DatabaseProvider | None,
) -> duckdb.DuckDBPyRelation:
    """
    Execute a query, using bigquery_query() if the accessor is a BigQuery database.

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

            # Need to escape single quotes in the query
            escaped_query = query.replace("'", "\\'")
            # path -> google cloud project
            project = database_provider.path

            if is_write_operation:
                # Use bigquery_execute for write operations
                wrapped_query = f"CALL bigquery_execute('{project}', '{escaped_query}')"
            else:
                # Use bigquery_query for read operations (supports views)
                wrapped_query = f"SELECT * FROM bigquery_query('{project}', '{escaped_query}')"

            return duckdb_client.query(wrapped_query)
        else:
            log.info(f"Database is of type '{database_provider.type}'. Using standard query execution.")

    return duckdb_client.query(query)


def parse_date(date: str, offset_days: int = 0) -> str:
    """
    Parses a date string which can be a relative terms like "today", "yesterday",
    or "tomorrow", actual dates, or relative dates like "today-2".

    Args:
        date: The date string to be parsed.
        offset_days: The number of days to be added/substracted.
    """
    date = str(date).lower()
    if date == "yesterday":
        offset_days -= 1
        return (dt.datetime.today() + dt.timedelta(days=offset_days)).date().isoformat()

    if date == "tomorrow":
        offset_days += 1
        return (dt.datetime.today() + dt.timedelta(days=offset_days)).date().isoformat()

    if regex_match := re.search(r"today([+-][0-9]+)", date):
        offset_days += int(regex_match[1])
        return (dt.datetime.today() + dt.timedelta(days=offset_days)).date().isoformat()

    return (dt.datetime.fromisoformat(date) + dt.timedelta(days=offset_days)).date().isoformat()


def parse_arg(arg: str) -> Union[str, int, bool]:
    if arg.lower() == "false":
        return False
    if arg.lower() == "true":
        return True

    if re.fullmatch(r"\d+(\.\d+)?", arg):  # if is int or float
        return literal_eval(arg)

    return arg


def to_set(value: Any) -> set:
    """
    Converts the input string to a set. The special case of one single string
    is also covered. Duplicates are also removed and for deterministic behavior,
    the values are sorted.

    It will, convert input as follows:
    - 1 -> {1}
    - True -> {True}
    - "toys" / '"toys"' -> {"toys"}
    - ("toys") / '("toys")' -> {"toys"}
    - ("toys", "shirt") / '("toys", "shirt")' -> {"shirt", "toys"}
    - ["toys"] -> {"toys"}
    - {"toys"} -> {"toys"}

    """
    try:
        value = literal_eval(value)
    except ValueError:
        pass
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        return {value}
    if isinstance(value, set):
        return value
    return set(value)
