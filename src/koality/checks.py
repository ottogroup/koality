"""Module containing data quality check classes."""

import abc
import datetime as dt
import math
import re
from typing import Any, Iterable, Literal, Optional

import duckdb

from koality.models import DatabaseProvider
from koality.utils import parse_date, to_set, execute_query

FLOAT_PRECISION = 4


class DataQualityCheck(abc.ABC):
    """
    Abstract class for all data quality checks. It provides generic methods
    relevant to all data quality check classes.

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
        monitor_only: bool = False,
        extra_info: str | None = None,
        **kwargs,
    ):
        self.database_accessor = database_accessor
        self.database_provider = database_provider
        self.table = table
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold
        self.monitor_only = monitor_only
        self.extra_info_string = f" {extra_info}" if extra_info else ""
        self.date_info_string = f" ({kwargs['date_info']})" if "date_info" in kwargs else ""

        self.status = "NOT_EXECUTED"
        self.message: str | None = None
        self.bytes_billed: int = 0

        # for where filter handling
        self.filters = self.get_filters(kwargs)

        self.shop_id = self.filters.get("shop_id", {}).get("value", "ALL_SHOPS")
        self.date_filter_value = self.filters.get("date", {}).get("value", dt.date.today().isoformat())

        if check_column is None:
            self.check_column = "*"
        else:
            self.check_column = check_column

        self.name = self.assemble_name()
        self.result: Optional[dict[str, Any]] = None

    @property
    def query(self):
        return self.assemble_query()

    @abc.abstractmethod
    def assemble_query(self) -> str:
        pass

    @abc.abstractmethod
    def assemble_data_exists_query(self) -> str:
        pass

    @abc.abstractmethod
    def assemble_name(self) -> str:
        pass

    def __repr__(self) -> str:
        if not hasattr(self, "shop_id"):
            return self.name

        return self.shop_id + "_" + self.name

    def data_check(self, duckdb_client: duckdb.DuckDBPyConnection) -> dict:
        """
        Performs a check if database tables used in the actual check
        contain data.

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
        empty_table = ""
        try:
            result = execute_query(self.assemble_data_exists_query(), duckdb_client, self.database_provider,).fetchone()
        except duckdb.Error:
            empty_table = f"Error while executing data check query on {self.table}"
        else:
            is_empty_table = bool(result and result[0])

        if not is_empty_table:
            return {}

        date = self.date_filter_value if hasattr(self, "date") else dt.datetime.today().isoformat()
        self.message = f"No data in {empty_table} on {date} for: {self.shop_id}"
        self.status = "FAIL"
        return {
            "DATE": date,
            "METRIC_NAME": "data_exists",
            "SHOP_ID": self.shop_id,
            "TABLE": empty_table,
        }

    def _check(self, duckdb_client: duckdb.DuckDBPyConnection, query: str) -> tuple[list[dict], str | None]:
        data = []
        error = None
        try:
            result = execute_query(query, duckdb_client, self.database_provider, )
        except duckdb.Error as e:
            error = str(e)
        else:
            data = [{col: val for col, val in zip(result.columns, row)} for row in result.fetchall()]
        return data, error

    def check(self, duckdb_client: duckdb.DuckDBPyConnection) -> dict:
        """
        Method that is actually performing the check of a data quality check
        object. If the check is set to `monitor_only`, the results of the
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
            self.message = f"{self.shop_id}: Metric {self.name} query errored with {error}"
        else:
            if self.monitor_only:
                result = "MONITOR_ONLY"
            else:
                success = check_value is not None and self.lower_threshold <= check_value <= self.upper_threshold
                result = "SUCCESS" if success else "FAIL"

        date = self.date_filter_value
        result_dict = {
            "DATE": date,
            "METRIC_NAME": self.name,
            "SHOP_ID": self.shop_id,
            "TABLE": self.table,
            "COLUMN": self.check_column,
            "VALUE": check_value,
            "LOWER_THRESHOLD": self.lower_threshold,
            "UPPER_THRESHOLD": self.upper_threshold,
            "RESULT": result,
        }

        if result_dict["RESULT"] == "FAIL":
            value_string = f"{result_dict['VALUE']:.{FLOAT_PRECISION}f}" if result_dict['VALUE'] is not None else "NULL"
            self.message = (
                f"{self.shop_id}: Metric {self.name} failed on {self.date_filter_value}{self.date_info_string} for {self.table}."
                f" Value {value_string} is not between {self.lower_threshold} and {self.upper_threshold}.{self.extra_info_string}"
            )
        self.status = result_dict["RESULT"]
        self.result = result_dict

        return result_dict

    def __call__(self, duckdb_client: duckdb.DuckDBPyConnection) -> dict:
        data_check_result = self.data_check(duckdb_client)
        if data_check_result:
            return data_check_result

        return self.check(duckdb_client)

    @staticmethod
    def get_filters(
        kwargs: dict,
        filter_col_suffix: str = r"_filter_column",
        filter_value_suffix: str = "_filter_value",
    ):
        """
        Generates a filter dict from kwargs using a regex pattern.
        Returns a dict of the format
            {"date": {"column": "date", "value": "2020-01-01"}, ...}
        """

        filters = {}

        # first, get all filter cols that are marked with filter_col_suffix,
        # e.g. shop_filter_column
        for key, value in kwargs.items():
            if match := re.fullmatch(r"(\w+)" + filter_col_suffix, key):
                filters[match[1]] = {"column": value}  # match[1] = "(\w+)" from above

        # next, find values for the filters ()
        for filter_key, filter_val in filters.items():
            for key, value in kwargs.items():
                if re.fullmatch(f"{filter_key}{filter_value_suffix}", key):
                    if filter_key == "date":
                        filter_val["value"] = parse_date(value, offset_days=kwargs.get("date_offset", 0))
                    else:
                        filter_val["value"] = value

                    break  # no need to loop any further

            else:  # no break
                raise ValueError(f"{filter_key}_filter_column has no corresponding value!")

        return filters

    @staticmethod
    def assemble_where_statement(filters: dict) -> str:
        """
        Generates the where statement for the check query using the specified
        filters.

        Args:
            filters: A dict containing filter specifications, e.g.,
            {'shop_id': {'column': 'shop_code', 'value': 'SHOP01'}, 'date': {'column': 'date', 'value': '2023-01-01'}}

        Returns:
            A WHERE statement to restrict the data being used for the check, e.g.,
            'WHERE shop_code = "SHOP01" AND date = "2023-01-01"'
        """

        if len(filters) == 0:
            return ""

        filters_statements = [
            4 * " " + f"{filter_dict['column']} = '{filter_dict['value']}'" for _, filter_dict in filters.items()
        ]

        return "WHERE\n" + "\nAND\n".join(filters_statements)


class ColumnTransformationCheck(DataQualityCheck, abc.ABC):
    """
    Abstract class for data quality checks performing checks on a specific
    column of a table.

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
        check_column: Optional[str] = None,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        self.transformation_name = transformation_name

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            extra_info=extra_info,
            **kwargs,
        )

    def assemble_name(self):
        return f"{self.check_column.split('.')[-1]}" + "_" + f"{self.transformation_name}"

    @abc.abstractmethod
    def transformation_statement(self) -> str:
        pass

    def query_boilerplate(self, metric_statement: str) -> str:
        return f"""
        SELECT
            {metric_statement}
        FROM
            {self.table}
        """

    def assemble_query(self) -> str:
        main_query = self.query_boilerplate(self.transformation_statement())

        if where_statement := self.assemble_where_statement(self.filters):
            return main_query + "\n" + where_statement

        return main_query

    def assemble_data_exists_query(self) -> str:
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
    """
    Checks the share of NULL values in a specific column of a table. It inherits from
    `koality.checks.ColumnTransformationCheck`, and thus, we refer to argument
    descriptions in its super class.


    Example:

    NullRatioCheck(
        date="2023-01-01",  # optional
        table="project.dataset.table",
        shop_id="SHOP01",  # optional
        check_column="orders",
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="date",  # optional
        lower_threshold=0.9,
        upper_threshold=1.0
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
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="null_ratio",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            extra_info=extra_info,
            **kwargs,
        )

    def transformation_statement(self) -> str:
        return f"""
            CASE 
                WHEN COUNT(*) = 0 THEN 0.0
                ELSE DIVIDE(COUNTIF({self.check_column} IS NULL), COUNT(*))
            END AS {self.name}
        """


class RegexMatchCheck(ColumnTransformationCheck):
    """
    Checks the share of values matching a regex in a specific column of a table. It
    inherits from `koality.checks.ColumnTransformationCheck`, and thus, we refer to
    argument descriptions in its super class, except for regex_to_match which is
    added in this subclass.

    Args:
        regex_to_match: The regular expression to be checked on check_column (e.g.,
                        "SHOP[0-9]{2}-.*" to check for a shop code prefix like "SHOP01-")

    Example:

    RegexMatchCheck(
        date="2023-01-01",  # optional
        table="project.dataset.table",
        check_column="^SHOP[0-9]{2}-.*"
        check_column="orders",
        date_filter_column="date",  # optional
        lower_threshold=0.9,
        upper_threshold=1.0
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
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        self.regex_to_match = regex_to_match

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="regex_match_ratio",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            extra_info=extra_info,
            **kwargs,
        )

    def transformation_statement(self) -> str:
        return f"""AVG(IF(REGEXP_CONTAINS({self.check_column}, "{self.regex_to_match}"), 1, 0)) AS {self.name}"""


class ValuesInSetCheck(ColumnTransformationCheck):
    """
    Checks the share of values that match any value of a value set in a specific
    column of a table. It inherits from `koality.checks.ColumnTransformationCheck`,
    and thus, we refer to argument descriptions in its super class, except for
    value set which is added in this subclass.

    Args:
        value_set: A list of values (or a string representation of such a list) to be checked.
                   Single values are also allowed. Examples for valid inputs:
                   - ["shoes", "clothing"]
                   - "clothing"
                   - '("shoes", "toys")'

    Example:

    ValuesInSetCheck(
        date="2023-01-01",  # optional
        table="project.dataset.table",
        shop_id="SHOP01",  # optional
        check_column="category",
        value_set='("toys", "shoes")',
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="date",  # optional
        lower_threshold=0.9,
        upper_threshold=1.0,
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
        monitor_only: bool = False,
        transformation_name: Optional[str] = None,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        self.value_set = to_set(value_set)
        if not self.value_set:
            raise ValueError("'value_set' must not be empty")
        self.value_set_string = f"({str(self.value_set)[1:-1]})"

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name=transformation_name if transformation_name else "values_in_set_ratio",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            extra_info=extra_info,
            **kwargs,
        )

    def transformation_statement(self) -> str:
        return f"""AVG(IF({self.check_column} IN {self.value_set_string}, 1, 0)) AS {self.name}"""


class RollingValuesInSetCheck(ValuesInSetCheck):
    """
    Checks the share of values that match any value of a value set in a specific
    column of a table similar to `ValuesInSetCheck`, but the share is computed for
    a longer time period (currently also including data of the 14 days before the
    actual check date). It inherits from `koality.checks.ValuesInSetCheck`,
    and thus, also from `koality.checks.ColumnTransformationCheck`,
    thus, we also refer to argument descriptions in its super class.

    Example:

    ValuesInSetCheck(
        date="2023-01-01",  # mandatory
        table="my-gcp-project.SHOP01.orders",
        shop_id="SHOP01",  # optional
        check_column="category",
        value_set='("toys", "shoes")',
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="DATE",  # mandatory
        lower_threshold=0.9,
        upper_threshold=1.0,
    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        value_set: str | bytes | Iterable,
        date_filter_column: str,
        date_filter_value: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        self.date_filter_column = date_filter_column
        self.date_filter_value = date_filter_value

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="rolling_values_in_set_ratio",
            table=table,
            value_set=value_set,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            date_filter_value=date_filter_value,
            date_filter_column=date_filter_column,
            extra_info=extra_info,
            **kwargs,
        )

        self.filters = {
            filter_name: filer_dict for filter_name, filer_dict in self.filters.items() if filter_name != "date"
        }

    def assemble_query(self) -> str:
        main_query = self.query_boilerplate(self.transformation_statement())

        main_query += (
            "WHERE\n    "
            + f"{self.date_filter_column} BETWEEN TIMESTAMP_SUB('{self.date_filter_value}', INTERVAL 14 DAY) AND '{self.date_filter_value}'"
        )  # TODO: maybe parameterize interval days

        if where_statement := self.assemble_where_statement(self.filters):
            return main_query + "\nAND\n" + where_statement.removeprefix("WHERE\n")

        return main_query


class DuplicateCheck(ColumnTransformationCheck):
    """
    Checks the number of duplicates for a specific column, i.e., all counts - distinct
    counts. It inherits from `koality.checks.ColumnTransformationCheck`, and thus, we
    refer to argument descriptions in its super class.

    Example:

    DuplicateCheck(
        date="2023-01-01",  # optional
        table="my-gcp-project.SHOP01.skufeed_latest",
        shop_id="SHOP01",  # optional
        check_column="sku_id",
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="DATE",  # optional
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
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="duplicates",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            extra_info=extra_info,
            **kwargs,
        )

    def transformation_statement(self) -> str:
        return f"COUNT(*) - COUNT(DISTINCT {self.check_column}) AS {self.name}"


class CountCheck(ColumnTransformationCheck):
    """
    Checks the number of rows or distinct values of a specific column. It inherits from
    `koality.checks.ColumnTransformationCheck`, and thus, we refer to argument
    descriptions in its super class, except for the `distinct` argument which is added in
    this subclass.

    Args:
        distinct: Indicates if the count should count all rows or only distinct values
                  of a specific column.
                  Note: distinct=True cannot be used with check_column="*".

    Example:

    CountCheck(
        date="2023-01-01",  # optional
        table="my-gcp-project.SHOP01.skufeed_latest",
        shop_id="SHOP01",  # optional
        check_column="sku_id",
        distinct=True,
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="DATE",  # optional
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
        distinct: bool = False,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        if check_column == "*" and distinct:
            raise ValueError("Cannot COUNT(DISTINCT *)! Either set check_column != '*' or distinct = False.")

        self.distinct = distinct

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name="distinct_count" if distinct else "count",
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            extra_info=extra_info,
            **kwargs,
        )

    def transformation_statement(self) -> str:
        if self.distinct:
            return f"COUNT(DISTINCT {self.check_column}) AS {self.name}"

        return f"COUNT({self.check_column}) AS {self.name}"

    def assemble_name(self):
        if self.check_column == "*":
            return f"row_{self.transformation_name}"

        return super().assemble_name()


class OccurrenceCheck(ColumnTransformationCheck):
    """
    Checks how often *any* value in a column occurs.
    It inherits from`koality.checks.ColumnTransformationCheck`, and thus, we refer to argument
      descriptions in its super class.
    Useful e.g. to check for a single product occurring unusually often (likely an error)

    Args:
        max_or_min: Check either the maximum or minimum occurrence of any value.
                    If you want to check if any value occurs more than x times, use 'max' and upper_threshold=x
                    If you want to check if any value occurs less than y times, use 'min' and lower_threshold=y

    Example:

    OccurrenceCheck(
        max_or_min="max,
        date="2023-01-01",  # optional
        table="my-gcp-project.SHOP01.skufeed_latest",
        shop_id="SHOP01",  # optional
        check_column="sku_id",
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="DATE",  # optional
        lower_threshold=0,
        upper_threshold=500
    )
    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        max_or_min: Literal["max", "min"],
        **kwargs,
    ):
        if max_or_min not in ("max", "min"):
            raise ValueError("'max_or_min' not one of supported modes 'min' or 'max'")
        self.max_or_min = max_or_min
        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name=f"occurrence_{max_or_min}",
            **kwargs
        )

    def transformation_statement(self) -> str:
        return f"{self.check_column}, COUNT(*) AS {self.name}"

    def assemble_query(self) -> str:
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
    """
    Checks the match rate between two tables after joining on specific columns.

    If left_join_columns (or right_join_columns) is defined, these columns will be
    used for joining the data. If not, join_columns will be used as fallback.

    Args:
        left_table: Name of BQ table for left part of join
                    (e.g., "my-gcp-project.SHOP01.identifier_base")
        right_table: Name of BQ table for right part of join
                     (e.g., "my-gcp-project.SHOP01.feature_baseline")
        check_column: Name of column to be checked (e.g., "product_number")
        join_columns: List of columns to join data on (e.g., ["PREDICTION_DATE", "product_number"])
        left_join_columns: List of columns of left table to join data on
                           (e.g., ["BQ_PARTITIONTIME", "productId"])
        right_join_columns: List of columns of right table to join data on
                            (e.g., ["PREDICTION_DATE", "product_number"])
        lower_threshold: Check will fail if check result < lower_threshold
        upper_threshold: Check will fail if check result > upper_threshold
        monitor_only: If True, no checks will be performed
        extra_info: Optional additional text that will be added to the end of the failure message

    Example:

    check = match_rate_check(
        date="2023-01-01",  # optional
        left_table="my-gcp-project.SHOP01.pdp_views",
        right_table="my-gcp-project.SHOP01.skufeed_latest",
        shop_id="SHOP01",  # optional
        join_columns_left=["DATE", "product_number_v2"],
        join_columns_right=["DATE", "product_number"],
        check_column="product_number",
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="DATE",  # optional
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
        monitor_only: bool = False,
        extra_info: str | None = None,
        **kwargs,
    ):
        self.left_table = left_table
        self.right_table = right_table

        if not (join_columns or (join_columns_left and join_columns_right)):
            raise ValueError(
                "No join_columns was provided. Use either join_columns or join_columns_left and join_columns_right"
            )

        # mypy typing does not understand that None is not possible, thus, we
        # add `or []`
        self.join_columns_left: list[str] = join_columns_left if join_columns_left else join_columns or []
        self.join_columns_right: list[str] = join_columns_right if join_columns_right else join_columns or []

        if not self.join_columns_right or not self.join_columns_left:
            raise ValueError(
                "No join_columns was provided. Use join_columns, join_columns_left, and/or join_columns_right"
            )

        if len(self.join_columns_left) != len(self.join_columns_right):
            raise ValueError(
                "join_columns_left and join_columns_right need to have equal length"
                f" ({len(self.join_columns_left)} vs. {len(self.join_columns_right)})."
            )

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            table=f"{self.left_table}_JOIN_{self.right_table}",
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            extra_info=extra_info,
            **kwargs,
        )

        self.filters_left = self.filters | self.get_filters(kwargs, filter_col_suffix="filter_column_left")
        self.filters_right = self.filters | self.get_filters(kwargs, filter_col_suffix="filter_column_right")

    def assemble_name(self):
        return f"{self.check_column.split('.')[-1]}_matchrate"

    def assemble_query(self) -> str:
        right_column_statement = ",\n    ".join(self.join_columns_right)

        join_on_statement = "\n    AND\n    ".join(
            [
                f"lefty.{left_col} = righty.{right_col.split('.')[-1]}"
                for left_col, right_col in zip(self.join_columns_left, self.join_columns_right, strict=False)
            ]
        )

        return f"""
        WITH
        righty AS (
        SELECT DISTINCT
            {right_column_statement},
            TRUE AS in_right_table
        FROM
            `{f"{self.database_accessor}." if self.database_accessor else ""}{self.right_table}`
        {self.assemble_where_statement(self.filters_right)}
        ),

        lefty AS (
        SELECT
            *
        FROM
            `{f"{self.database_accessor}." if self.database_accessor else ""}{self.left_table}`
        {self.assemble_where_statement(self.filters_left)}
        )

        SELECT
            ROUND(SAFE_DIVIDE(COUNTIF(in_right_table IS TRUE), COUNT(*)), 3) AS {self.name}
        FROM
            lefty
        LEFT JOIN
            righty
        ON
            {join_on_statement}
        """

    def assemble_data_exists_query(self) -> str:
        """
        First checks left, then right table for data.

        Returns:
            Empty table name or empty string
        """

        return f"""
        WITH
        righty AS (
            SELECT
                COUNT(*) AS right_counter,
            FROM
                `{f"{self.database_accessor}." if self.database_accessor else ""}{self.right_table}`
            {self.assemble_where_statement(self.filters_right)}
        ),

        lefty AS (
            SELECT
                COUNT(*) AS left_counter,
            FROM
                `{f"{self.database_accessor}." if self.database_accessor else ""}{self.left_table}`
            {self.assemble_where_statement(self.filters_left)}
        )

        SELECT
            IF(
                (SELECT * FROM lefty) > 0,
                IF((SELECT * FROM righty) > 0, '', '{self.right_table}'),
                '{self.left_table}'
            ) AS empty_table
        """  # noqa: S608


class RelCountChangeCheck(DataQualityCheck):  # TODO: (non)distinct counts parameter?
    """
    Checks the relative change of a count in comparison to the average counts of a
    number of historic days before the check date.

    Args:
        date: The date where the check should be performed (e.g., "2023-01-01")
        table: Name of BQ table (e.g., "my-gcp-project.SHOP01.feature_category")
        check_column: Name of column to be checked (e.g., "category")
        rolling_days: The number of historic days to be taken into account for
                      the historic average baseline for the comparison (e.g., 7).
        date_filter_column: The name of the date column
        lower_threshold: Check will fail if check result < lower_threshold
        upper_threshold: Check will fail if check result > upper_threshold
        monitor_only: If True, no checks will be performed
        extra_info: Optional additional text that will be added to the end of the failure message

    Example:

    RelCountChangeCheck(
        date="2023-01-01",  # mandatory
        table="my-gcp-project.SHOP01.skufeed_latest",
        shop_id="SHOP01",  # optional
        check_column="sku_id",
        rolling_days=7,
        shop_id_filter_column="shop_code",  # optional
        date_filter_column="DATE",  # mandatory
        lower_threshold=-0.15,
        upper_threshold=0.15
    )
    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        table: str,
        check_column: str,
        rolling_days: int,
        date_filter_column: str,
        date_filter_value: str,
        lower_threshold: float = -math.inf,
        upper_threshold: float = math.inf,
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        self.rolling_days = rolling_days
        self.date_filter_value = date_filter_value
        self.date_filter_column = date_filter_column

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            table=table,
            check_column=check_column,
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
            monitor_only=monitor_only,
            date_filter_column=date_filter_column,
            date_filter_value=date_filter_value,
            extra_info=extra_info,
            **kwargs,
        )

        self.filters = {
            filter_name: filer_dict for filter_name, filer_dict in self.filters.items() if filter_name != "date"
        }

    def assemble_name(self):
        return f"{self.check_column.split('.')[-1]}" + "_count_change"

    def assemble_query(self) -> str:
        where_statement = self.assemble_where_statement(self.filters).replace("WHERE", "AND")

        return f"""
        WITH base AS (
        SELECT
            {self.date_filter_column},
            COUNT(DISTINCT {self.check_column}) AS dist_cnt
        FROM
            `{f"{self.database_accessor}." if self.database_accessor else ""}{self.table}`
        WHERE
            {self.date_filter_column} BETWEEN TIMESTAMP_SUB("{self.date_filter_value}", INTERVAL {self.rolling_days} DAY)
            AND "{self.date_filter_value}"
        {where_statement}
        GROUP BY
            {self.date_filter_column}
        ),

        rolling_avgs AS (
        SELECT
            AVG(dist_cnt) AS rolling_avg
        FROM
            base
        WHERE
            {self.date_filter_column} BETWEEN TIMESTAMP_SUB("{self.date_filter_value}", INTERVAL {self.rolling_days} DAY)
        AND
            TIMESTAMP_SUB("{self.date_filter_value}", INTERVAL 1 DAY)
        ),

        -- Helper is needed to cover case where no current data is available
        dist_cnt_helper AS (
          SELECT
            MAX(dist_cnt) AS dist_cnt
          FROM
            (
                SELECT dist_cnt FROM base WHERE {self.date_filter_column} = "{self.date_filter_value}"
                UNION ALL
                SELECT 0 AS dist_cnt
            )
        )

        SELECT
            ROUND(SAFE_DIVIDE((dist_cnt - rolling_avg), rolling_avg), 3) AS {self.name}
        FROM
            dist_cnt_helper
        JOIN
            rolling_avgs
        ON TRUE
        """  # noqa: S608

    def assemble_data_exists_query(self) -> str:
        data_exists_query = f"""
        SELECT
            IF(COUNT(*) > 0, '', '{self.table}') AS empty_table
        FROM
            {f"{self.database_accessor}." if self.database_accessor else ""}{self.table}
        """

        where_statement = self.assemble_where_statement(self.filters)
        if where_statement:
            return f"{data_exists_query}\n{where_statement} AND {self.date_filter_column} = '{self.date_filter_value}'"
        return f"{data_exists_query}\nWHERE {self.date_filter_column} = '{self.date_filter_value}'"


class IqrOutlierCheck(ColumnTransformationCheck):
    """
    Checks if the date-specific value of a column is an outlier based on the
    interquartile range (IQR) method. It inherits from `koality.checks.ColumnTransformationCheck`,
    and thus, we refer to argument descriptions in its super class, except for the
    `date` and `date_filter_column` arguments which are added in this sub class.

    The IQR method is based on the 25th and 75th percentiles of the data. The
    thresholds are calculated as follows:
    - lower_threshold = q25 - iqr_factor * (q75 - q25)


    Example:

    IqrOutlierCheck(
        check_column="num_orders",
        table="my-gcp-project.SHOP01.orders",
        date="2023-01-01",
        date_filter_column="DATE",  # optional
        interval_days=14,
        how="both",  # check both upper and lower outliers
        iqr_factor=1.5
    )
    """

    def __init__(
        self,
        database_accessor: str,
        database_provider: DatabaseProvider | None,
        check_column: str,
        table: str,
        date_filter_column: str,
        date_filter_value: str,
        interval_days: int,
        how: Literal["both", "upper", "lower"],
        iqr_factor: float,
        monitor_only: bool = False,
        extra_info: Optional[str] = None,
        **kwargs,
    ):
        self.date_filter_column = date_filter_column
        self.date_filter_value = date_filter_value
        if interval_days < 1:
            raise ValueError("interval_days must be at least 1")
        self.interval_days = int(interval_days)
        if how not in ["both", "upper", "lower"]:
            raise ValueError("how must be one of 'both', 'upper', 'lower'")
        self.how = how
        # reasonable lower bound for iqr_factor
        if iqr_factor < 1.5:
            raise ValueError("iqr_factor must be at least 1.5")
        self.iqr_factor = float(iqr_factor)

        super().__init__(
            database_accessor=database_accessor,
            database_provider=database_provider,
            transformation_name=f"outlier_iqr_{self.how}_{str(self.iqr_factor).replace('.', '_')}",
            table=table,
            check_column=check_column,
            lower_threshold=-math.inf,
            upper_threshold=math.inf,
            monitor_only=monitor_only,
            extra_info=extra_info,
            date_filter_column=date_filter_column,
            date_filter_value=date_filter_value,
            **kwargs,
        )

        self.filters = {
            filter_name: filer_dict for filter_name, filer_dict in self.filters.items() if filter_name != "date"
        }

    # UDF for calculating exact percentiles in BigQuery
    # https://medium.com/@rakeshmohandas/how-to-calculate-exact-quantiles-percentiles-in-bigquery-using-6465d69fc136
    _percentile_udf = '''
    CREATE TEMP FUNCTION
    ExactPercentile(arr ARRAY<FLOAT64>,
    percentile FLOAT64)
    RETURNS FLOAT64
    LANGUAGE js AS """
    var sortedArray = arr.slice().sort(function(a, b) {
    return a - b;
    });
    var index = (sortedArray.length - 1) * percentile;
    var lower = Math.floor(index);
    var upper = Math.ceil(index);
    if (lower === upper) {
    return sortedArray[lower];
    }
    var fraction = index - lower;
    return sortedArray[lower] * (1 - fraction) + sortedArray[upper] * fraction;
    """;
    '''

    def transformation_statement(self) -> str:
        # TODO: currently we only raise an error if there is no data for the date
        #       we could also raise an error if there is not enough data for the
        #       IQR calculation
        where_statement = ""
        filter_columns = ""
        if self.filters:
            filter_columns = ",\n".join([v["column"] for k, v in self.filters.items()])
            filter_columns = ",\n" + filter_columns
            where_statement = self.assemble_where_statement(self.filters)
            where_statement = "\nAND\n" + where_statement.removeprefix("WHERE\n")
        return f"""
          DECLARE slice_count INT64;
          SET slice_count = (SELECT COUNT(*) FROM {self.table} WHERE {self.date_filter_column} = "{self.date_filter_value}");
          IF slice_count = 0 THEN
            RAISE USING MESSAGE = 'No data for {self.date_filter_value} in {self.table}!';
          END IF;

          {self._percentile_udf}

          WITH
          raw AS (
            SELECT
                DATE({self.date_filter_column}) AS {self.date_filter_column},
                {self.check_column}
                {filter_columns}
            FROM
                {self.table}
            WHERE
                DATE({self.date_filter_column}) BETWEEN DATE_SUB("{self.date_filter_value}", INTERVAL {self.interval_days} DAY)
                AND "{self.date_filter_value}"
                {where_statement}
          ),
          compare AS (
            SELECT * FROM raw WHERE {self.date_filter_column} < "{self.date_filter_value}"
          ),
          slice AS (
            SELECT * FROM raw WHERE {self.date_filter_column} = "{self.date_filter_value}"
          ),
          percentiles AS (
            SELECT
              ExactPercentile(ARRAY_AGG(CAST({self.check_column} AS FLOAT64)), 0.25) AS q25,
              ExactPercentile(ARRAY_AGG(CAST({self.check_column} AS FLOAT64)), 0.75) AS q75
            FROM
              compare
          ),
          stats AS (
            SELECT
              * except ({self.check_column}),
              {self.check_column} AS {self.name},
              (percentiles.q25 - {self.iqr_factor} * (percentiles.q75 - percentiles.q25)) AS lower_threshold,
              (percentiles.q75 + {self.iqr_factor} * (percentiles.q75 - percentiles.q25)) AS upper_threshold,
            FROM
              slice
            LEFT JOIN percentiles
            ON TRUE
          )
        """  # noqa: S608

    def query_boilerplate(self, metric_statement: str) -> str:
        return f"""
            {metric_statement}

            SELECT
                *
            FROM
                stats
        """

    def _check(self, duckdb_client: duckdb.DuckDBPyConnection, query: str) -> tuple[list[dict], str | None]:
        result, error = super()._check(duckdb_client, query)
        # overwrite the lower and upper thresholds as required
        if result:
            if self.how in ["both", "lower"]:
                self.lower_threshold = result[0]["lower_threshold"]
            if self.how in ["both", "upper"]:
                self.upper_threshold = result[0]["upper_threshold"]
        return result, error

    def assemble_data_exists_query(self) -> str:
        data_exists_query = f"""
        SELECT
            IF(COUNTIF({self.check_column} IS NOT NULL) > 0, '', '{self.table}') AS empty_table
        FROM
            {f"{self.database_accessor}." if self.database_accessor else ""}{self.table}
        """
        where_statement = self.assemble_where_statement(self.filters)
        if where_statement:
            where_statement = f"{where_statement} AND {self.date_filter_column} = '{self.date_filter_value}'"
        else:
            where_statement = f"WHERE {self.date_filter_column} = '{self.date_filter_value}'"
        return f"{data_exists_query}\n{where_statement}"
