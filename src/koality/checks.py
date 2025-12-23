"""Module containing data quality check classes."""

import abc
import datetime as dt
import math
from collections.abc import Iterable
from typing import Any, Literal

import duckdb

from koality.exceptions import KoalityError
from koality.models import DatabaseProvider, FilterConfig
from koality.utils import execute_query, parse_date, to_set

FLOAT_PRECISION = 4


class DataQualityCheck(abc.ABC):
    """Abstract class for all data quality checks.

    Provides generic methods relevant to all data quality check classes.

    Args:
        table: Name of BQ table (e.g., "project.dataset.table")
        check_column: Name of column to be checked (e.g., "category")
        lower_threshold: Check will fail if check result < lower_threshold
        upper_threshold: Check will fail if check result > upper_threshold
        monitor_only: If True, no checks will be performed
        extra_info: Optional additional text that will be added to the end of the failure message

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str | None = None,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the data quality check with configuration parameters."""
        self.database_accessor = database_accessor
        self.database_provider = database_provider
        self.table = table
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        self.monitor_only = monitor_only
        self.extra_info_string = f" {extra_info}" if extra_info else ""
        self.date_info_string = f" ({date_info})" if date_info else ""

        self.status = "NOT_EXECUTED"
        self.message: str | None = None
        self.bytes_billed: int = 0

        # Identifier format configuration
        self.identifier_format = identifier_format

        # for where filter handling
        self.filters = self.get_filters(filters or {})

        # Find identifier filter by type and format based on identifier_format setting
        identifier_filter_result = self.get_identifier_filter(self.filters)
        if identifier_filter_result:
            filter_name, filter_config = identifier_filter_result
            value = filter_config.get("value", "ALL")
            column = filter_config.get("column", "")

            if self.identifier_format == "identifier":
                # Format as "column=value"
                self.identifier = f"{column}={value}" if column else str(value)
                self.identifier_column = "IDENTIFIER"
            elif self.identifier_format == "filter_name":
                # Use filter name as column, value as-is
                self.identifier = str(value)
                self.identifier_column = filter_name.upper()
            else:  # column_name
                # Use database column name as column, value as-is
                self.identifier = str(value)
                self.identifier_column = column.upper() if column else "IDENTIFIER"
        else:
            self.identifier = "ALL"
            self.identifier_column = "IDENTIFIER"

        # Find date filter by type and store the filter dict
        date_filter_result = self.get_date_filter(self.filters)
        if date_filter_result:
            self.date_filter = date_filter_result[1]
        else:
            self.date_filter = None

        if check_column is None:
            self.check_column = "*"
        else:
            self.check_column = check_column

        self.name = self.assemble_name()
        self.result: dict[str, Any] | None = None

    @property
    def query(self) -> str:
        """Return the assembled SQL query for this check."""
        return self.assemble_query()

    @abc.abstractmethod
    def assemble_query(self) -> str:
        """Assemble and return the SQL query for this check."""

    @abc.abstractmethod
    def assemble_data_exists_query(self) -> str:
        """Assemble and return the SQL query to check if data exists."""

    @abc.abstractmethod
    def assemble_name(self) -> str:
        """Assemble and return the name for this check."""

    def __repr__(self) -> str:
        """Return string representation combining identifier and check name."""
        if not hasattr(self, "identifier"):
            return self.name

        return f"{self.identifier}_{self.name}"

    def data_check(self, duckdb_client: duckdb.DuckDBPyConnection) -> dict:
        """Check if database tables used in the actual check contain data.

        Note: The returned result dict and failure message will be later
        aggregated in order to avoid duplicates in the reported failures.

        Args:
            duckdb_client: DuckDB client for interacting with DuckDB

        Returns:
            If there is a table without data, a dict containing information about
            missing data will be returned, otherwise an empty dict indicating that
            data exists.

        """
        is_empty_table = False
        try:
            result = execute_query(
                self.assemble_data_exists_query(),
                duckdb_client,
                self.database_provider,
            ).fetchone()
        except duckdb.Error:
            empty_table = f"Error while executing data check query on {self.table}"
        else:
            empty_table = result[0] if result else self.table
            is_empty_table = bool(empty_table)

        if not is_empty_table:
            return {}

        date = self.date_filter["value"] if self.date_filter else dt.datetime.now(tz=dt.UTC).date().isoformat()
        self.message = f"No data in {empty_table} on {date} for: {self.identifier}"
        self.status = "FAIL"
        return {
            "DATE": date,
            "METRIC_NAME": "data_exists",
            self.identifier_column: self.identifier,
            "TABLE": empty_table,
        }

    def _check(self, duckdb_client: duckdb.DuckDBPyConnection, query: str) -> tuple[list[dict], str | None]:
        data = []
        error = None
        try:
            result = execute_query(
                query,
                duckdb_client,
                self.database_provider,
            )
        except duckdb.Error as e:
            error = str(e)
        else:
            data = [dict(zip(result.columns, row, strict=False)) for row in result.fetchall()]
        return data, error

    def check(self, duckdb_client: duckdb.DuckDBPyConnection) -> dict:
        """Perform the data quality check and return results.

        If the check is set to `monitor_only`, the results of the
        check will be documented without comparison to the lower and
        upper thresholds.

        Args:
            duckdb_client: DuckDB client for interacting with DuckDB

        Returns:
            A dict containing all information and the result of the check

        """
        result, error = self._check(duckdb_client, self.query)

        check_value = result[0][self.name] if result else None
        check_value = float(check_value) if check_value is not None else None
        if error:
            result = "ERROR"
            self.message = f"{self.identifier}: Metric {self.name} query errored with {error}"
        elif self.monitor_only:
            result = "MONITOR_ONLY"
        else:
            success = check_value is not None and self.lower_threshold <= check_value <= self.upper_threshold
            result = "SUCCESS" if success else "FAIL"

        date = self.date_filter["value"] if self.date_filter else dt.datetime.now(tz=dt.UTC).date().isoformat()
        result_dict = {
            "DATE": date,
            "METRIC_NAME": self.name,
            self.identifier_column: self.identifier,
            "TABLE": self.table,
            "COLUMN": self.check_column,
            "VALUE": check_value,
            "LOWER_THRESHOLD": self.lower_threshold,
            "UPPER_THRESHOLD": self.upper_threshold,
            "RESULT": result,
        }

        if result_dict["RESULT"] == "FAIL":
            value_string = f"{result_dict['VALUE']:.{FLOAT_PRECISION}f}" if result_dict["VALUE"] is not None else "NULL"
            self.message = (
                f"{self.identifier}: Metric {self.name} failed on {date}{self.date_info_string} "
                f"for {self.table}. Value {value_string} is not between {self.lower_threshold} and "
                f"{self.upper_threshold}.{self.extra_info_string}"
            )
        self.status = result_dict["RESULT"]
        self.result = result_dict

        return result_dict

    def __call__(self, duckdb_client: duckdb.DuckDBPyConnection) -> dict:
        """Execute the data quality check and return results."""
        data_check_result = self.data_check(duckdb_client)
        if data_check_result:
            return data_check_result

        return self.check(duckdb_client)

    @staticmethod
    def get_filters(filters_config: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """Generate a filter dict from filter configurations.

        Args:
            filters_config: Dictionary containing filter configurations.

        Example YAML:
            filters:
              partition_date:
                column: BQ_PARTITIONTIME
                value: yesterday-2  # 2 days before yesterday
                type: date  # auto-parses value as date
              shop_id:
                column: shopId
                value: EC0601
                type: identifier
              revenue:
                column: total_revenue
                value: 1000
                operator: ">="
              category:
                column: category
                value: ["toys", "electronics"]
                operator: "IN"

        Returns:
            A dict of the format:
                {"partition_date": {"column": "DATE", "value": "2020-01-01", "operator": "=", "type": "date"}, ...}

        """
        filters: dict[str, dict[str, Any]] = {}

        for filter_name, config in filters_config.items():
            if isinstance(config, FilterConfig):
                config_dict = config.model_dump()
            elif isinstance(config, dict):
                config_dict = config
            else:
                config_dict = {"value": config}

            column = config_dict.get("column")
            if column is None:
                continue

            value = config_dict.get("value")
            filter_type = config_dict.get("type", "other")

            # Auto-parse date values when type is "date" or parse_as_date is True
            should_parse = filter_type == "date" or config_dict.get("parse_as_date", False)
            if should_parse and value is not None:
                value = parse_date(str(value))

            operator = config_dict.get("operator", "=")

            filters[filter_name] = {
                "column": column,
                "value": value,
                "operator": operator,
                "type": filter_type,
            }

        return filters

    @staticmethod
    def get_date_filter(filters: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]] | None:
        """Find the date filter (type='date') from the filters dict.

        Args:
            filters: The filters dict from get_filters().

        Returns:
            A tuple of (filter_name, filter_config) if found, None otherwise.

        """
        for name, config in filters.items():
            if config.get("type") == "date":
                return name, config
        return None

    @staticmethod
    def get_identifier_filter(filters: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]] | None:
        """Find the identifier filter (type='identifier') from the filters dict.

        Args:
            filters: The filters dict from get_filters().

        Returns:
            A tuple of (filter_name, filter_config) if found, None otherwise.

        """
        for name, config in filters.items():
            if config.get("type") == "identifier":
                return (name, config)
        return None

    @staticmethod
    def _format_filter_value(
        value: str | float | list | tuple | set,
        operator: str,
    ) -> str:
        """Format a filter value for SQL based on the operator.

        Args:
            value: The filter value (can be a single value or list for IN/NOT IN).
            operator: The SQL operator being used.

        Returns:
            Formatted SQL value string.

        """
        if operator in ("IN", "NOT IN"):
            if isinstance(value, (list, tuple, set)):
                formatted_values = ", ".join(f"'{v}'" for v in value)
                return f"({formatted_values})"
            return f"('{value}')"

        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)

        return f"'{value}'"

    @staticmethod
    def assemble_where_statement(filters: dict[str, dict[str, Any]]) -> str:
        """Generate the where statement for the check query using the specified filters.

        Args:
            filters: A dict containing filter specifications, e.g.,
                `{
                    'identifier': {
                        'column': 'shop_code',
                        'value': 'SHOP01',
                        'operator': '=',
                        'type': 'identifier'
                    },
                    'date': {
                        'column': 'date',
                        'value': '2023-01-01',
                        'operator': '=',
                        'type': 'date'
                    },
                    'revenue': {
                        'column': 'total_revenue',
                        'value': 1000,
                        'operator': '>='
                    },
                    'category': {
                        'column': 'category',
                        'value': ['toys', 'electronics'],
                        'operator': 'IN'
                    }
                }`

        Returns:
            A WHERE statement to restrict the data being used for the check, e.g.,
            'WHERE shop_code = 'SHOP01' AND date = '2023-01-01' AND total_revenue >= 1000'

        """
        if len(filters) == 0:
            return ""

        filters_statements = []
        for filter_dict in filters.values():
            column = filter_dict["column"]
            value = filter_dict["value"]
            operator = filter_dict.get("operator", "=")

            # Handle NULL values with IS NULL / IS NOT NULL
            if value is None:
                if operator == "!=":
                    filters_statements.append(f"    {column} IS NOT NULL")
                else:
                    filters_statements.append(f"    {column} IS NULL")
                continue

            formatted_value = DataQualityCheck._format_filter_value(value, operator)
            filters_statements.append(f"    {column} {operator} {formatted_value}")

        return "WHERE\n" + "\nAND\n".join(filters_statements)


class ColumnTransformationCheck(DataQualityCheck, abc.ABC):
    """Abstract class for data quality checks performing checks on a specific column of a table.

    Args:
        transformation_name: The name to refer to this check (in combination with check_column)
        table: Name of BQ table (e.g., "project.dataset.table")
        check_column: Name of column to be checked (e.g., "category")
        lower_threshold: Check will fail if check result < lower_threshold
        upper_threshold: Check will fail if check result > upper_threshold
        monitor_only: If True, no checks will be performed
        extra_info: Optional additional text that will be added to the end of the failure message

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        transformation_name: str,
        table: str,
        check_column: str | None = None,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the column transformation check."""
        self.transformation_name = transformation_name

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def assemble_name(self) -> str:
        """Return the check name combining column and transformation."""
        return f"{self.check_column.split('.')[-1]}_{self.transformation_name}"

    @abc.abstractmethod
    def transformation_statement(self) -> str:
        """Return the SQL transformation statement for this check."""

    def query_boilerplate(self, metric_statement: str) -> str:
        """Return the base SQL query structure with the given metric statement."""
        return f"""
        SELECT
            {metric_statement}
        FROM
            {self.table}
        """

    def assemble_query(self) -> str:
        """Assemble the complete SQL query for this check."""
        main_query = self.query_boilerplate(self.transformation_statement())

        if where_statement := self.assemble_where_statement(self.filters):
            return main_query + "\n" + where_statement

        return main_query

    def assemble_data_exists_query(self) -> str:
        """Assemble the SQL query to check if data exists in the table."""
        data_exists_query = f"""
        SELECT
            IF(COUNT(*) > 0, '', '{self.table}') AS empty_table
        FROM
            {self.table}
        """

        if where_statement := self.assemble_where_statement(self.filters):
            return f"{data_exists_query}\n{where_statement}"

        return data_exists_query


class NullRatioCheck(ColumnTransformationCheck):
    """Check the share of NULL values in a specific column of a table.

    Inherits from ColumnTransformationCheck; see its documentation for argument descriptions.

    Example:
    NullRatioCheck(
        database_accessor="project.dataset",
        database_provider=None,
        table="project.dataset.table",
        check_column="orders",
        filters={
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
            "date": {"column": "date", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=0.9,
        upper_threshold=1.0,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the null ratio check."""
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="null_ratio",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for calculating null ratio."""
        return f"""
            CASE
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE ROUND(COUNTIF({self.check_column} IS NULL) / COUNT(*), 3)
            END AS {self.name}
        """


class RegexMatchCheck(ColumnTransformationCheck):
    """Check the share of values matching a regex in a specific column of a table.

    Inherits from ColumnTransformationCheck; see its documentation for argument descriptions.

    Args:
        regex_to_match: The regular expression to be checked on check_column (e.g.,
                        "SHOP[0-9]{2}-.*" to check for a shop code prefix like "SHOP01-")

    Example:
    RegexMatchCheck(
        database_accessor="project.dataset",
        database_provider=None,
        table="project.dataset.table",
        check_column="orders",
        regex_to_match="^SHOP[0-9]{2}-.*",
        filters={"date": {"column": "date", "value": "2023-01-01", "type": "date"}},
        lower_threshold=0.9,
        upper_threshold=1.0,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        regex_to_match: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the regex match check."""
        self.regex_to_match = regex_to_match

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="regex_match_ratio",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for calculating regex match ratio."""
        return f"""AVG(IF(REGEXP_MATCHES({self.check_column}, '{self.regex_to_match}'), 1, 0)) AS {self.name}"""


class ValuesInSetCheck(ColumnTransformationCheck):
    """Check the share of values that match any value of a value set in a column.

    Inherits from ColumnTransformationCheck; see its documentation for argument descriptions.

    Args:
        value_set: A list of values (or a string representation of such a list) to be checked.
                   Single values are also allowed. Examples for valid inputs:
                   - ["shoes", "clothing"]
                   - "clothing"
                   - '("shoes", "toys")'

    Example:
    ValuesInSetCheck(
        database_accessor="project.dataset",
        database_provider=None,
        table="project.dataset.table",
        check_column="category",
        value_set='("toys", "shoes")',
        filters={
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
            "date": {"column": "date", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=0.9,
        upper_threshold=1.0,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        value_set: str | bytes | Iterable,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        monitor_only: bool = False,
        transformation_name: str | None = None,
        extra_info: str | None = None,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
    ) -> None:
        """Initialize the values in set check."""
        self.value_set = to_set(value_set)
        if not self.value_set:
            msg = "'value_set' must not be empty"
            raise KoalityError(msg)
        self.value_set_string = f"({str(self.value_set)[1:-1]})"

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name=transformation_name if transformation_name else "values_in_set_ratio",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for calculating values in set ratio."""
        return f"""AVG(IF({self.check_column} IN {self.value_set_string}, 1, 0)) AS {self.name}"""


class RollingValuesInSetCheck(ValuesInSetCheck):
    """Check share of values matching a value set over a rolling time period.

    Similar to `ValuesInSetCheck`, but the share is computed for a longer time period
    (currently also including data of the 14 days before the actual check date).
    It inherits from `koality.checks.ValuesInSetCheck`, and thus, also from
    `koality.checks.ColumnTransformationCheck`, so we refer to argument descriptions
    in its super class.

    Args:
        filters: Filter configuration dict. Must include a 'date' filter with column and value.

    Example:
    RollingValuesInSetCheck(
        database_accessor="my-gcp-project.SHOP01",
        database_provider=None,
        table="my-gcp-project.SHOP01.orders",
        check_column="category",
        value_set='("toys", "shoes")',
        filters={
            "partition_date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
        },
        lower_threshold=0.9,
        upper_threshold=1.0,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        value_set: str | bytes | Iterable,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the rolling values in set check."""
        # Find date filter by type
        filters = filters or {}
        date_filter = None
        for config in filters.values():
            cfg = config.model_dump() if isinstance(config, FilterConfig) else config
            if cfg.get("type") == "date":
                date_filter = cfg
                break

        if not date_filter or not date_filter.get("column") or date_filter.get("value") is None:
            msg = "RollingValuesInSetCheck requires a filter with type='date'"
            raise KoalityError(msg)

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="rolling_values_in_set_ratio",
            table=table,
            value_set=value_set,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

        # Remove date filter from WHERE clause (it's used in the rolling window SQL, not WHERE)
        self.filters = {name: cfg for name, cfg in self.filters.items() if cfg.get("type") != "date"}

    def assemble_query(self) -> str:
        """Assemble query with rolling date range for values in set check."""
        main_query = self.query_boilerplate(self.transformation_statement())
        date_col = self.date_filter["column"]
        date_val = self.date_filter["value"]

        main_query += (
            "WHERE\n    "
            f"{date_col} BETWEEN (DATE '{date_val}' - INTERVAL 14 DAY) AND '{date_val}'"
        )  # TODO: maybe parameterize interval days

        if where_statement := self.assemble_where_statement(self.filters):
            return main_query + "\nAND\n" + where_statement.removeprefix("WHERE\n")

        return main_query


class DuplicateCheck(ColumnTransformationCheck):
    """Check the number of duplicates for a specific column.

    Counts all rows minus distinct counts. Inherits from ColumnTransformationCheck.

    Example:
    DuplicateCheck(
        database_accessor="my-gcp-project.SHOP01",
        database_provider=None,
        table="my-gcp-project.SHOP01.skufeed_latest",
        check_column="sku_id",
        filters={
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=0.0,
        upper_threshold=0.0,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the duplicate check."""
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="duplicates",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for counting duplicates."""
        return f"COUNT(*) - COUNT(DISTINCT {self.check_column}) AS {self.name}"


class CountCheck(ColumnTransformationCheck):
    """Check the number of rows or distinct values of a specific column.

    Inherits from `koality.checks.ColumnTransformationCheck`, and thus, we refer to
    argument descriptions in its super class, except for the `distinct` argument which
    is added in this subclass.

    Args:
        distinct: Indicates if the count should count all rows or only distinct values
                  of a specific column.
                  Note: distinct=True cannot be used with check_column="*".

    Example:
    CountCheck(
        database_accessor="my-gcp-project.SHOP01",
        database_provider=None,
        table="my-gcp-project.SHOP01.skufeed_latest",
        check_column="sku_id",
        distinct=True,
        filters={
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=10000.0,
        upper_threshold=99999.0,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        distinct: bool = False,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the count check."""
        if check_column == "*" and distinct:
            msg = "Cannot COUNT(DISTINCT *)! Either set check_column != '*' or distinct = False."
            raise KoalityError(msg)

        self.distinct = distinct

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="distinct_count" if distinct else "count",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for counting rows or distinct values."""
        if self.distinct:
            return f"COUNT(DISTINCT {self.check_column}) AS {self.name}"

        return f"COUNT({self.check_column}) AS {self.name}"

    def assemble_name(self) -> str:
        """Return the check name, using 'row_' prefix for wildcard columns."""
        if self.check_column == "*":
            return f"row_{self.transformation_name}"

        return super().assemble_name()


class AverageCheck(ColumnTransformationCheck):
    """Compute the average (AVG) of a numeric column for the filtered rows.

    Inherits from ColumnTransformationCheck. Thresholds apply to the computed average.
    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the average check."""
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="avg",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for computing the average."""
        return f"AVG({self.check_column}) AS {self.name}"


class MaxCheck(ColumnTransformationCheck):
    """Compute the maximum (MAX) of a column for the filtered rows.

    Inherits from ColumnTransformationCheck. Thresholds apply to the computed maximum.
    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the max check."""
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="max",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for computing the maximum."""
        return f"MAX({self.check_column}) AS {self.name}"


class MinCheck(ColumnTransformationCheck):
    """Compute the minimum (MIN) of a column for the filtered rows.

    Inherits from ColumnTransformationCheck. Thresholds apply to the computed minimum.
    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the min check."""
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="min",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for computing the minimum."""
        return f"MIN({self.check_column}) AS {self.name}"


class OccurrenceCheck(ColumnTransformationCheck):
    """Check how often any value in a column occurs.

    Inherits from `koality.checks.ColumnTransformationCheck`, and thus, we refer to argument
    descriptions in its super class.
    Useful e.g. to check for a single product occurring unusually often (likely an error).

    Args:
        max_or_min: Check either the maximum or minimum occurrence of any value.
                    If you want to check if any value occurs more than x times, use 'max' and upper_threshold=x
                    If you want to check if any value occurs less than y times, use 'min' and lower_threshold=y

    Example:
    OccurrenceCheck(
        database_accessor="my-gcp-project.SHOP01",
        database_provider=None,
        max_or_min="max",
        table="my-gcp-project.SHOP01.skufeed_latest",
        check_column="sku_id",
        filters={
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        lower_threshold=0,
        upper_threshold=500,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        max_or_min: Literal["max", "min"],
        table: str,
        check_column: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the occurrence check."""
        if max_or_min not in ("max", "min"):
            msg = "'max_or_min' must be one of supported modes 'min' or 'max'"
            raise KoalityError(msg)
        self.max_or_min = max_or_min
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name=f"occurrence_{max_or_min}",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

    def transformation_statement(self) -> str:
        """Return the SQL statement for counting occurrences."""
        return f"{self.check_column}, COUNT(*) AS {self.name}"

    def assemble_query(self) -> str:
        """Assemble query to find max or min occurrence of any value."""
        # Since koality checks only the first entry, the table with value + count_occurence is
        # ordered DESC/ASC depending on whether max/min occurence is supposed to be checked.
        order = {"max": "DESC", "min": "ASC"}[self.max_or_min]
        return f"""
            {self.query_boilerplate(self.transformation_statement())}
            {self.assemble_where_statement(self.filters)}
            GROUP BY {self.check_column}
            ORDER BY {self.name} {order}
            LIMIT 1  -- only the first entry is needed
        """


class MatchRateCheck(DataQualityCheck):
    """Checks the match rate between two tables after joining on specific columns.

    If left_join_columns (or right_join_columns) is defined, these columns will be
    used for joining the data. If not, join_columns will be used as fallback.

    Args:
        left_table: Name of table for left part of join
                    (e.g., "my-gcp-project.SHOP01.identifier_base")
        right_table: Name of table for right part of join
                     (e.g., "my-gcp-project.SHOP01.feature_baseline")
        check_column: Name of column to be checked (e.g., "product_number")
        join_columns: List of columns to join data on (e.g., ["PREDICTION_DATE", "product_number"])
        join_columns_left: List of columns of left table to join data on
                           (e.g., ["BQ_PARTITIONTIME", "productId"])
        join_columns_right: List of columns of right table to join data on
                            (e.g., ["PREDICTION_DATE", "product_number"])
        lower_threshold: Check will fail if check result < lower_threshold
        upper_threshold: Check will fail if check result > upper_threshold
        monitor_only: If True, no checks will be performed
        extra_info: Optional additional text that will be added to the end of the failure message

    Example:
    MatchRateCheck(
        database_accessor="my-gcp-project.SHOP01",
        database_provider=None,
        left_table="my-gcp-project.SHOP01.pdp_views",
        right_table="my-gcp-project.SHOP01.skufeed_latest",
        join_columns_left=["DATE", "product_number_v2"],
        join_columns_right=["DATE", "product_number"],
        check_column="product_number",
        filters={
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
        },
        filters_left={
            "status": {"column": "order_status", "value": "completed"},
        },
        filters_right={
            "active": {"column": "is_active", "value": True},
        },
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        left_table: str,
        right_table: str,
        check_column: str,
        join_columns: list[str] | None = None,
        join_columns_left: list[str] | None = None,
        join_columns_right: list[str] | None = None,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        monitor_only: bool = False,
        extra_info: str | None = None,
        filters: dict[str, Any] | None = None,
        filters_left: dict[str, Any] | None = None,
        filters_right: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
    ) -> None:
        """Initialize the match rate check."""
        self.left_table = left_table
        self.right_table = right_table

        if not (join_columns or (join_columns_left and join_columns_right)):
            msg = "No join_columns was provided. Use either join_columns or join_columns_left and join_columns_right"
            raise KoalityError(msg)

        # mypy typing does not understand that None is not possible, thus, we
        # add `or []`
        self.join_columns_left: list[str] = join_columns_left if join_columns_left else join_columns or []
        self.join_columns_right: list[str] = join_columns_right if join_columns_right else join_columns or []

        if not self.join_columns_right or not self.join_columns_left:
            msg = "No join_columns was provided. Use join_columns, join_columns_left, and/or join_columns_right"
            raise KoalityError(msg)

        if len(self.join_columns_left) != len(self.join_columns_right):
            msg = (
                f"join_columns_left and join_columns_right need to have equal length"
                f" ({len(self.join_columns_left)} vs. {len(self.join_columns_right)})."
            )
            raise KoalityError(msg)

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            table=f"{self.left_table}_JOIN_{self.right_table}",
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

        # Support table-specific filters via filters_left and filters_right
        self.filters_left = self.filters | self.get_filters(filters_left or {})
        self.filters_right = self.filters | self.get_filters(filters_right or {})

    def assemble_name(self) -> str:
        """Return the check name for match rate."""
        return f"{self.check_column.split('.')[-1]}_matchrate"

    def assemble_query(self) -> str:
        """Assemble the SQL query for calculating match rate between tables."""
        right_column_statement = ",\n    ".join(self.join_columns_right)

        join_on_statement = "\n    AND\n    ".join(
            [
                f"lefty.{left_col} = righty.{right_col.split('.')[-1]}"
                for left_col, right_col in zip(self.join_columns_left, self.join_columns_right, strict=False)
            ],
        )

        return f"""
        WITH
            righty AS (
                SELECT DISTINCT
                    {right_column_statement},
                    TRUE AS in_right_table
                FROM
                    {f"{self.database_accessor}." if self.database_accessor else ""}{self.right_table}
                {self.assemble_where_statement(self.filters_right)}
            ),
            lefty AS (
                SELECT
                    *
                FROM
                    {f"{self.database_accessor}." if self.database_accessor else ""}{self.left_table}
                {self.assemble_where_statement(self.filters_left)}
            )

            SELECT
                CASE
                    WHEN COUNT(*) = 0 THEN 0.0
                    ELSE ROUND(COUNTIF(in_right_table IS TRUE) / COUNT(*), 3)
                END AS {self.name}
            FROM
                lefty
            LEFT JOIN
                righty
            ON
                {join_on_statement}
        """

    def assemble_data_exists_query(self) -> str:
        """First checks left, then right table for data.

        Returns:
            Empty table name or empty string

        """
        return f"""
        WITH
        righty AS (
            SELECT
                COUNT(*) AS right_counter,
            FROM
                {f"{self.database_accessor}." if self.database_accessor else ""}{self.right_table}
            {self.assemble_where_statement(self.filters_right)}
        ),

        lefty AS (
            SELECT
                COUNT(*) AS left_counter,
            FROM
                {f"{self.database_accessor}." if self.database_accessor else ""}{self.left_table}
            {self.assemble_where_statement(self.filters_left)}
        )

        SELECT
            IF(
                (SELECT * FROM lefty) > 0,
                IF((SELECT * FROM righty) > 0, '', '{self.right_table}'),
                '{self.left_table}'
            ) AS empty_table
        """


class RelCountChangeCheck(DataQualityCheck):  # TODO: (non)distinct counts parameter?
    """Check the relative change of a count compared to historic average.

    Compares the count to the average counts of a number of historic days before
    the check date.

    Args:
        table: Name of table (e.g., "my-gcp-project.SHOP01.feature_category")
        check_column: Name of column to be checked (e.g., "category")
        rolling_days: The number of historic days to be taken into account for
                      the historic average baseline for the comparison (e.g., 7).
        lower_threshold: Check will fail if check result < lower_threshold
        upper_threshold: Check will fail if check result > upper_threshold
        monitor_only: If True, no checks will be performed
        extra_info: Optional additional text that will be added to the end of the failure message
        filters: Filter configuration dict. Must include a 'date' filter with column and value.

    Example:
    RelCountChangeCheck(
        database_accessor="my-gcp-project.SHOP01",
        database_provider=None,
        table="my-gcp-project.SHOP01.skufeed_latest",
        check_column="sku_id",
        rolling_days=7,
        filters={
            "partition_date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
        },
        lower_threshold=-0.15,
        upper_threshold=0.15,
    )

    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        rolling_days: int,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the relative count change check."""
        self.rolling_days = rolling_days

        # Find date filter by type
        filters = filters or {}
        date_filter = None
        for config in filters.values():
            cfg = config.model_dump() if isinstance(config, FilterConfig) else config
            if cfg.get("type") == "date":
                date_filter = cfg
                break

        if not date_filter or not date_filter.get("column") or date_filter.get("value") is None:
            msg = "RelCountChangeCheck requires a filter with type='date'"
            raise KoalityError(msg)

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

        # Remove date filter from WHERE clause (it's used in the rolling window SQL, not WHERE)
        self.filters = {name: cfg for name, cfg in self.filters.items() if cfg.get("type") != "date"}

    def assemble_name(self) -> str:
        """Return the check name for count change."""
        return f"{self.check_column.split('.')[-1]}_count_change"

    def assemble_query(self) -> str:
        """Assemble the SQL query for calculating relative count change."""
        where_statement = self.assemble_where_statement(self.filters).replace("WHERE", "AND")
        date_col = self.date_filter["column"]
        date_val = self.date_filter["value"]

        return f"""
        WITH
            base AS (
                SELECT
                    {date_col},
                    COUNT(DISTINCT {self.check_column}) AS dist_cnt
                FROM
                    {f"{self.database_accessor}." if self.database_accessor else ""}{self.table}
                WHERE
                    {date_col} BETWEEN (DATE '{date_val}' - INTERVAL {self.rolling_days} DAY)
                    AND '{date_val}'
                {where_statement}
                GROUP BY
                    {date_col}
            ),
            rolling_avgs AS (
                SELECT
                    AVG(dist_cnt) AS rolling_avg
                FROM
                    base
                WHERE
                    {date_col} BETWEEN (DATE '{date_val}' - INTERVAL {self.rolling_days} DAY)
                AND
                    (DATE '{date_val}' - INTERVAL 1 DAY)
            ),

            -- Helper is needed to cover case where no current data is available
            dist_cnt_helper AS (
                SELECT
                    MAX(dist_cnt) AS dist_cnt
                FROM
                    (
                        SELECT dist_cnt FROM base WHERE {date_col} = '{date_val}'
                        UNION ALL
                        SELECT 0 AS dist_cnt
                    )
            )

            SELECT
                CASE
                    WHEN rolling_avg = 0 THEN 0.0
                    ELSE ROUND((dist_cnt - rolling_avg) / rolling_avg, 3)
                END AS {self.name}
            FROM
                dist_cnt_helper
            JOIN
                rolling_avgs
            ON TRUE
        """

    def assemble_data_exists_query(self) -> str:
        """Assemble the SQL query to check if data exists for the check date."""
        data_exists_query = f"""
        SELECT
            IF(COUNT(*) > 0, '', '{self.table}') AS empty_table
        FROM
            {f"{self.database_accessor}." if self.database_accessor else ""}{self.table}
        """
        date_col = self.date_filter["column"]
        date_val = self.date_filter["value"]

        where_statement = self.assemble_where_statement(self.filters)
        if where_statement:
            return f"{data_exists_query}\n{where_statement} AND {date_col} = '{date_val}'"
        return f"{data_exists_query}\nWHERE {date_col} = '{date_val}'"


class IqrOutlierCheck(ColumnTransformationCheck):
    """Check if a column value is an outlier based on the interquartile range (IQR) method.

    Inherits from `koality.checks.ColumnTransformationCheck`, and thus, we refer to
    argument descriptions in its super class.

    The IQR method is based on the 25th and 75th percentiles of the data. The
    thresholds are calculated as follows:
        - lower_threshold = q25 - iqr_factor * (q75 - q25)
        - upper_threshold = q75 + iqr_factor * (q75 - q25)

    Args:
        filters: Filter configuration dict. Must include a filter with type='date'.
        interval_days: Number of historic days to use for IQR calculation.
        how: Check mode - 'both', 'upper', or 'lower' outliers.
        iqr_factor: Multiplier for IQR range (minimum 1.5).

    Example:
    IqrOutlierCheck(
        database_accessor="my-gcp-project.SHOP01",
        database_provider=None,
        check_column="num_orders",
        table="my-gcp-project.SHOP01.orders",
        interval_days=14,
        how="both",
        iqr_factor=1.5,
        filters={
            "partition_date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
            "identifier": {"column": "shop_code", "value": "SHOP01", "type": "identifier"},
        },
    )

    """

    MIN_IQR_FACTOR = 1.5

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        check_column: str,
        table: str,
        interval_days: int,
        how: Literal["both", "upper", "lower"],
        iqr_factor: float,
        *,
        filters: dict[str, Any] | None = None,
        identifier_format: str = "identifier",
        date_info: str | None = None,
        extra_info: str | None = None,
        monitor_only: bool = False,
    ) -> None:
        """Initialize the IQR outlier check."""
        # Find date filter by type
        filters = filters or {}
        date_filter = None
        for config in filters.values():
            cfg = config.model_dump() if isinstance(config, FilterConfig) else config
            if cfg.get("type") == "date":
                date_filter = cfg
                break

        if not date_filter or not date_filter.get("column") or date_filter.get("value") is None:
            msg = "IqrOutlierCheck requires a filter with type='date'"
            raise KoalityError(msg)

        if interval_days < 1:
            msg = "interval_days must be at least 1"
            raise KoalityError(msg)
        self.interval_days = int(interval_days)
        if how not in ["both", "upper", "lower"]:
            msg = "how must be one of 'both', 'upper', 'lower'"
            raise KoalityError(msg)
        self.how = how
        # reasonable lower bound for iqr_factor
        if iqr_factor < self.MIN_IQR_FACTOR:
            msg = f"iqr_factor must be at least {self.MIN_IQR_FACTOR}"
            raise KoalityError(msg)
        self.iqr_factor = float(iqr_factor)

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name=f"outlier_iqr_{self.how}_{str(self.iqr_factor).replace('.', '_')}",
            table=table,
            check_column=check_column,
            lower_threshold=-math.inf,
            upper_threshold=math.inf,
            filters=filters,
            identifier_format=identifier_format,
            date_info=date_info,
            extra_info=extra_info,
            monitor_only=monitor_only,
        )

        # Remove date filter from WHERE clause (it's used in the interval SQL, not WHERE)
        self.filters = {name: cfg for name, cfg in self.filters.items() if cfg.get("type") != "date"}

    def transformation_statement(self) -> str:
        """Return the SQL statement for IQR-based outlier detection."""
        # TODO: currently we only raise an error if there is no data for the date
        #       we could also raise an error if there is not enough data for the
        #       IQR calculation
        where_statement = ""
        filter_columns = ""
        date_col = self.date_filter["column"]
        date_val = self.date_filter["value"]

        if self.filters:
            filter_columns = ",\n".join([v["column"] for v in self.filters.values()])
            filter_columns = ",\n" + filter_columns
            where_statement = self.assemble_where_statement(self.filters)
            where_statement = "\nAND\n" + where_statement.removeprefix("WHERE\n")
        return f"""
        WITH
            base AS (
                SELECT
                    DATE({date_col}) AS {date_col},
                    {self.check_column}
                    {filter_columns}
                FROM
                    {self.table}
                WHERE
                    DATE({date_col}) BETWEEN (DATE '{date_val}' - INTERVAL {self.interval_days} DAY)
                    AND DATE '{date_val}'
                    {where_statement}
            ),
            compare AS (
                SELECT * FROM base WHERE {date_col} < '{date_val}'
            ),
            slice AS (
                SELECT * FROM base WHERE {date_col} = '{date_val}'
            ),
            percentiles AS (
                SELECT
                  QUANTILE_CONT(CAST({self.check_column} AS FLOAT), 0.25) AS q25,
                  QUANTILE_CONT(CAST({self.check_column} AS FLOAT), 0.75) AS q75
                FROM
                  compare
            ),
            stats AS (
                SELECT
                  * exclude ({self.check_column}),
                  {self.check_column} AS {self.name},
                  (percentiles.q25 - {self.iqr_factor} * (percentiles.q75 - percentiles.q25)) AS lower_threshold,
                  (percentiles.q75 + {self.iqr_factor} * (percentiles.q75 - percentiles.q25)) AS upper_threshold,
                FROM
                  slice
                LEFT JOIN percentiles
                ON TRUE
            )
        """

    def query_boilerplate(self, metric_statement: str) -> str:
        """Return the query structure for IQR outlier detection."""
        return f"""
            {metric_statement}

            SELECT
                *
            FROM
                stats
        """

    def _check(self, duckdb_client: duckdb.DuckDBPyConnection, query: str) -> tuple[list[dict], str | None]:
        """Execute check and update thresholds from IQR calculation."""
        result, error = super()._check(duckdb_client, query)
        # overwrite the lower and upper thresholds as required
        if result:
            if self.how in ["both", "lower"]:
                self.lower_threshold = result[0]["lower_threshold"]
            if self.how in ["both", "upper"]:
                self.upper_threshold = result[0]["upper_threshold"]
        return result, error

    def assemble_data_exists_query(self) -> str:
        """Assemble the query to check if data exists for IQR outlier detection."""
        data_exists_query = f"""
        SELECT
            IF(COUNTIF({self.check_column} IS NOT NULL) > 0, '', '{self.table}') AS empty_table
        FROM
            {f"{self.database_accessor}." if self.database_accessor else ""}{self.table}
        """
        date_col = self.date_filter["column"]
        date_val = self.date_filter["value"]

        where_statement = self.assemble_where_statement(self.filters)
        if where_statement:
            where_statement = f"{where_statement} AND {date_col} = '{date_val}'"
        else:
            where_statement = f"WHERE {date_col} = '{date_val}'"
        return f"{data_exists_query}\n{where_statement}"
