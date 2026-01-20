"""Module containing the actual check execution logic."""

import datetime
import logging
from collections import defaultdict
from pathlib import Path

import duckdb
from tqdm import tqdm

from koality.checks import (
    AverageCheck,
    CountCheck,
    DataQualityCheck,
    DuplicateCheck,
    IqrOutlierCheck,
    MatchRateCheck,
    MaxCheck,
    MinCheck,
    NullRatioCheck,
    OccurrenceCheck,
    RegexMatchCheck,
    RelCountChangeCheck,
    RollingValuesInSetCheck,
    ValuesInSetCheck,
)
from koality.exceptions import DatabaseError
from koality.models import CHECK_TYPE, Config
from koality.utils import execute_query, format_threshold, identify_database_provider

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CHECK_MAP: dict[CHECK_TYPE, type[DataQualityCheck]] = {
    "NullRatioCheck": NullRatioCheck,
    "RegexMatchCheck": RegexMatchCheck,
    "ValuesInSetCheck": ValuesInSetCheck,
    "RollingValuesInSetCheck": RollingValuesInSetCheck,
    "DuplicateCheck": DuplicateCheck,
    "CountCheck": CountCheck,
    "AverageCheck": AverageCheck,
    "MaxCheck": MaxCheck,
    "MinCheck": MinCheck,
    "OccurrenceCheck": OccurrenceCheck,
    "MatchRateCheck": MatchRateCheck,
    "RelCountChangeCheck": RelCountChangeCheck,
    "IqrOutlierCheck": IqrOutlierCheck,
}

# Mapping from DuckDB types to database-specific types
# Structure: {duckdb_type: {database_type: target_type}} # noqa: ERA001
# Uses defaultdict so unknown database types fall back to the original duckdb_type
DATA_TYPES: dict[str, dict[str, str]] = defaultdict(
    dict,
    {
        "VARCHAR": defaultdict(
            lambda: "VARCHAR",
            {
                "bigquery": "STRING",
            },
        ),
        "DATE": defaultdict(
            lambda: "DATE",
            {
                "bigquery": "DATE",
            },
        ),
        "TIMESTAMP": defaultdict(
            lambda: "TIMESTAMP",
            {
                "bigquery": "TIMESTAMP",
            },
        ),
        "NUMERIC": defaultdict(
            lambda: "NUMERIC",
            {
                "bigquery": "FLOAT64",
            },
        ),
    },
)


class CheckExecutor:
    """Provide all the logic to actually run checks.

    Contains methods to execute checks, to create the failed checks log,
    and to export DQM results to a database table.

    Args:
        config: Koality configuration object
        duckdb_client: DuckDB client for interacting with DuckDB (optional).
            If not provided, an in-memory connection will be created.

    """

    def __init__(
        self,
        config: Config,
        duckdb_client: duckdb.DuckDBPyConnection | None = None,
        **kwargs: object,
    ) -> None:
        """Initialize the check executor with configuration and optional DuckDB client."""
        self.config = config
        if duckdb_client is not None:
            self.duckdb_client = duckdb_client
        else:
            self.duckdb_client = duckdb.connect(":memory:")
            self.duckdb_client.query(self.config.database_setup)
        self.database_provider = None
        if self.config.database_accessor:
            self.database_provider = identify_database_provider(self.duckdb_client, self.config.database_accessor)

        self.kwargs = kwargs

        self.checks: list[DataQualityCheck] = []
        self.check_failed = False

        self.jobs_: list = []

        self.result_dicts: list[dict] = []
        self.result_table = self.config.defaults.result_table
        self.persist_results = self.config.defaults.persist_results
        self.log_path = self.config.defaults.log_path

        self._data_existence_cache: dict[tuple, dict] = {}

    @staticmethod
    def aggregate_values(value_list: list[str]) -> str:
        """Join a list of values into a comma-separated, sorted, deduplicated string."""
        return ", ".join(sorted(set(value_list)))

    @staticmethod
    def _get_dataset_cache_key(check_instance: DataQualityCheck) -> tuple:
        """Generate a unique cache key for data existence checks.

        The cache key is based on attributes that uniquely identify a dataset:
        - table name
        - database_accessor
        - date from date_filter (if present)
        - filters that affect data existence

        Args:
            check_instance: The check instance to generate a cache key for

        Returns:
            A hashable tuple representing the unique dataset identifier

        """
        # Get the table name
        table = check_instance.table

        # Get database accessor
        database_accessor = check_instance.database_accessor or ""

        # Get date from date_filter
        date_value = None
        if check_instance.date_filter:
            date_value = check_instance.date_filter.get("value")

        # Convert filters dict to a frozenset for hashability
        # We only need filters that affect data existence (all of them)
        filters_items = []
        if check_instance.filters:
            for filter_name, filter_config in sorted(check_instance.filters.items()):
                # Create a hashable representation of each filter
                filter_tuple = (
                    filter_name,
                    filter_config.get("column"),
                    str(filter_config.get("value")),
                    filter_config.get("operator"),
                    filter_config.get("type"),
                )
                filters_items.append(filter_tuple)

        # Special handling for MatchRateCheck which has table-specific filters
        # These are used in the data existence query and must be part of the cache key
        if isinstance(check_instance, MatchRateCheck):
            # Add left table filters
            for filter_name, filter_config in sorted(check_instance.filters_left.items()):
                filter_tuple = (
                    f"left_{filter_name}",
                    filter_config.get("column"),
                    str(filter_config.get("value")),
                    filter_config.get("operator"),
                    filter_config.get("type"),
                )
                filters_items.append(filter_tuple)

            # Add right table filters
            for filter_name, filter_config in sorted(check_instance.filters_right.items()):
                filter_tuple = (
                    f"right_{filter_name}",
                    filter_config.get("column"),
                    str(filter_config.get("value")),
                    filter_config.get("operator"),
                    filter_config.get("type"),
                )
                filters_items.append(filter_tuple)

        filters_key = frozenset(filters_items)

        return table, database_accessor, date_value, filters_key

    def get_data_requirements(self) -> defaultdict[str, defaultdict[str, set]]:  # noqa: C901
        """Aggregate data requirements from all checks.

        This method collects all required tables, columns, and filter configurations
        from the checks. The requirements are loaded into the in-memory DuckDB database
        from which the checks will read their data.

        Returns:
            A nested dictionary mapping table names to required columns and filter configurations.

        """
        data_requirements = defaultdict(lambda: defaultdict(set))
        for check in self.checks:
            table_name = check.table
            check_filters = check.filters
            # Add check-specific columns and filter columns to the requirements
            if check.check_column and check.check_column != "*":
                data_requirements[table_name]["columns"].add(check.check_column)
            for _filter in check_filters.values():
                if "column" in _filter:
                    data_requirements[table_name]["columns"].add(_filter["column"])

            # For MatchRateCheck, add columns from both left and right tables
            if isinstance(check, MatchRateCheck):
                data_requirements[check.left_table]["columns"].update(check.join_columns_left)
                data_requirements[check.right_table]["columns"].update(check.join_columns_right)
                for _filter in check.filters_left.values():
                    if "column" in _filter:
                        data_requirements[check.left_table]["columns"].add(_filter["column"])
                for _filter in check.filters_right.values():
                    if "column" in _filter:
                        data_requirements[check.right_table]["columns"].add(_filter["column"])

            if isinstance(check, IqrOutlierCheck):
                check_filters = {k: v for k, v in check.filters.items() if v.get("type") != "date"}

            # Store unique filter configurations for each table
            filter_key = frozenset((name, frozenset(config.items())) for name, config in check_filters.items())
            data_requirements[table_name]["filters"].add(filter_key)
        return data_requirements

    def fetch_data_into_memory(self, data_requirements: defaultdict[str, defaultdict[str, set]]) -> None:
        """Fetch all required data into DuckDB memory before executing checks.

        This method aggregates all data requirements (tables, columns, filters) from the checks,
        constructs efficient bulk-SELECT queries, and executes them to populate in-memory DuckDB tables.
        This avoids executing a separate query for each check.
        """
        for table, requirements in data_requirements.items():
            columns = ", ".join(sorted(set(requirements["columns"])))
            if not columns:
                columns = "*"

            # Combine all unique filter groups with OR
            all_filters_sql = set()
            for filter_group in requirements["filters"]:
                filter_dict = {name: dict(config) for name, config in filter_group}
                where_clause = DataQualityCheck.assemble_where_statement(filter_dict)
                # Extract the conditions from the WHERE clause
                if where_clause.strip().startswith("WHERE"):
                    conditions = where_clause.strip()[len("WHERE") :].strip()
                    if conditions:
                        all_filters_sql.add(f"({conditions})")

            # Build the final WHERE clause
            final_where_clause = ""
            if all_filters_sql:
                final_where_clause = "WHERE " + " OR ".join(all_filters_sql)

            # Construct the bulk SELECT query
            select_query = f"""
            SELECT {columns}
            FROM "{table}"
            {final_where_clause}
            """  # noqa: S608

            try:
                # Execute the query to get the data as a DuckDB relation
                relation = execute_query(  # noqa: F841
                    select_query,
                    self.duckdb_client,
                    self.config.database_accessor,
                    self.database_provider,
                )
                # Create a table in DuckDB from the relation
                self.duckdb_client.sql(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM relation')  # noqa: S608
                msg = f"Successfully loaded data for table {table} into memory."
                log.info(msg)
            except duckdb.Error as e:
                msg = f"Failed to load data for table {table}"
                log.exception(msg)
                # Decide if we should raise the error or continue
                raise DatabaseError(msg) from e

    def execute_checks(self) -> None:
        """Instantiate and execute all checks.

        When walking through the different checks, parameters are updated using global
        defaults, check defaults of bundles, and a check's parameters. Check results
        are stored in a results dict for further processing.
        """
        results = []

        # First, instantiate all the checks
        for check_bundle in self.config.check_bundles:
            for check_config in check_bundle.checks:
                check_factory = CHECK_MAP[check_config.check_type]
                check_kwargs = check_config.model_dump(exclude={"check_type"}, exclude_none=True)

                if check_config.check_type == "IqrOutlierCheck":
                    check_kwargs.pop("lower_threshold", None)
                    check_kwargs.pop("upper_threshold", None)

                check_kwargs["database_accessor"] = self.config.database_accessor
                check_kwargs["database_provider"] = self.database_provider
                check_kwargs["identifier_format"] = self.config.defaults.identifier_format
                check_kwargs["identifier_placeholder"] = self.config.defaults.identifier_placeholder
                check_instance = check_factory(**check_kwargs)
                self.checks.append(check_instance)

        # Now, fetch all data into memory if using a database accessor
        # this is required so that we can run all checks against the in-memory data and rely solely on
        # DuckDB functionality like regexp_matches and others
        if self.config.database_accessor:
            data_requirements = self.get_data_requirements()
            self.fetch_data_into_memory(data_requirements)

        # Then, execute the checks against the in-memory data
        for check_instance in tqdm(self.checks, desc="Executing checks", unit="check"):
            # From now on, we query the in-memory DB, so we don't need the accessor
            cache_key = self._get_dataset_cache_key(check_instance)
            if cache_key not in self._data_existence_cache:
                data_check_result = check_instance.data_check(self.duckdb_client)
                self._data_existence_cache[cache_key] = data_check_result
            else:
                data_check_result = self._data_existence_cache[cache_key]

            if data_check_result:
                results.append(data_check_result)
            else:
                results.append(check_instance.check(self.duckdb_client))

        for check in self.checks:
            if check.status in ("FAIL", "ERROR"):
                self.check_failed = True
                break

        self.result_dicts = results

    def _aggregate_checks_msgs(self, msg_list: list[str]) -> list[str]:
        """Aggregate a list of (failure) check messages.

        If data tables to be checked do not contain any data, a specific failure
        will be created. As a larger number of such failures can be created (for
        different checks or for different shop IDs), we aggregate missing data
        failures, grouping them by table and date and joining all distinct shop
        IDs to a comma-separated list.

        Args:
            msg_list: A list of failure messages.

        Returns:
            A list of failure messages with aggregated missing data messages.

        """
        # Other messages to be left untouched
        msgs_other = [msg for msg in msg_list if not msg.startswith("No data")]

        # Missing data messages to be aggregated
        msgs_no_data = [msg for msg in msg_list if msg.startswith("No data")]

        # Group and aggregate messages
        grouped_data = {}
        expected_parts_count = 2
        for msg in msgs_no_data:
            parts = msg.split(":", 1)
            if len(parts) == expected_parts_count:
                table_part = parts[0]
                identifier = parts[1].strip()
                if table_part not in grouped_data:
                    grouped_data[table_part] = []
                grouped_data[table_part].append(identifier)

        msgs_no_data = [
            f"{table_part}: {self.aggregate_values(identifiers)}" for table_part, identifiers in grouped_data.items()
        ]

        return msgs_no_data + msgs_other

    def _aggregate_result_dicts(self, result_dicts: list[dict]) -> list[dict]:
        """Aggregate a list of check result dicts.

        If data tables to be checked do not contain any data, a specific failure
        will be created. As a larger number of such failures can be created (for
        different checks or for different identifiers), we aggregate missing data
        failures, grouping them by table and date and joining all distinct identifiers
        to a comma-separated list.

        Args:
            result_dicts: A list of check result dicts.

        Returns:
            A list of check result dicts with aggregated missing data results.

        """
        # Other results to be left untouched
        result_other = [
            result for result in result_dicts if result["METRIC_NAME"] not in ("data_exists", "table_exists")
        ]

        # Missing data results to be aggregated (includes table_exists)
        result_no_data = [result for result in result_dicts if result["METRIC_NAME"] in ("data_exists", "table_exists")]

        # Group and aggregate messages
        if not result_no_data:
            return result_other

        # Get identifier column name from the first result
        identifier_column = self.checks[0].identifier_column if self.checks else "IDENTIFIER"

        grouped_data = {}
        for result in result_no_data:
            key = (result["DATE"], result["METRIC_NAME"], result["TABLE"])
            if key not in grouped_data:
                grouped_data[key] = {
                    "DATE": result["DATE"],
                    "METRIC_NAME": result["METRIC_NAME"],
                    "TABLE": result["TABLE"],
                    "_identifier_values": [],
                }
            grouped_data[key]["_identifier_values"].append(result[identifier_column])

        result_no_data = [
            {
                "DATE": value["DATE"],
                "METRIC_NAME": value["METRIC_NAME"],
                "TABLE": value["TABLE"],
                identifier_column: self.aggregate_values(value["_identifier_values"]),
                "COLUMN": None,
                "VALUE": None,
                "LOWER_THRESHOLD": None,
                "UPPER_THRESHOLD": None,
                "RESULT": "FAIL",
            }
            for value in grouped_data.values()
        ]

        return result_no_data + result_other

    def get_failed_checks_msg(self) -> str:
        """Get an aggregated message for all failed checks.

        Uses the message attribute of all checks and aggregates it.

        Returns:
            Aggregated, sorted, newline separated messages of failed checks.

        """
        failed_checks_msgs = [check.message for check in self.checks if check.message]
        failed_checks_msgs = self._aggregate_checks_msgs(failed_checks_msgs)
        failed_checks_msgs.sort()

        return "\n".join(failed_checks_msgs)

    def load_to_database(self) -> None:
        """Persist koality's DQM results in a BQ table.

        The result table is partitioned by DATE. DQM data is always appended to table.
        """
        if self.result_table is None:
            log.info("result_table is None. Results were not persisted.")
            return

        if len(self.result_dicts) == 0:
            log.info("No entries in results from checks, so no results were persisted.")
            return

        now = datetime.datetime.now(tz=datetime.UTC)

        # Get the identifier column name from the first check (all checks have the same column name)
        identifier_column = self.checks[0].identifier_column if self.checks else "IDENTIFIER"

        # Copy rows first, cause INSERT_TIMESTAMP and AUTO value is only a BQ feature and not needed anywhere else
        results_with_it: list[dict] = self._aggregate_result_dicts(self.result_dicts).copy()
        # Add INSERT_TIMESTAMP col with AUTO value to automatically set insert_timestamp (BQ feature)
        for row in results_with_it:
            row["INSERT_TIMESTAMP"] = now

        if self.config.database_accessor:
            query_create_or_replace_table = f"""
                CREATE TABLE IF NOT EXISTS {self.result_table} (
                    DATE {DATA_TYPES["DATE"][self.database_provider.type]},
                    METRIC_NAME {DATA_TYPES["VARCHAR"][self.database_provider.type]},
                    `TABLE` {DATA_TYPES["VARCHAR"][self.database_provider.type]},
                    {identifier_column} {DATA_TYPES["VARCHAR"][self.database_provider.type]},
                    `COLUMN` {DATA_TYPES["VARCHAR"][self.database_provider.type]},
                    VALUE {DATA_TYPES["VARCHAR"][self.database_provider.type]},
                    LOWER_THRESHOLD {DATA_TYPES["NUMERIC"][self.database_provider.type]},
                    UPPER_THRESHOLD {DATA_TYPES["NUMERIC"][self.database_provider.type]},
                    RESULT {DATA_TYPES["VARCHAR"][self.database_provider.type]},
                    INSERT_TIMESTAMP {DATA_TYPES["TIMESTAMP"][self.database_provider.type]} DEFAULT CURRENT_TIMESTAMP
                )
            """
            try:
                # make sure table exists
                execute_query(
                    query_create_or_replace_table,
                    self.duckdb_client,
                    self.config.database_accessor,
                    self.database_provider,
                )
            except duckdb.Error as e:
                msg = f"Could not create or replace table {self.result_table}"
                raise DatabaseError(msg) from e

            # Convert results_with_it to VALUES clause

            values_clause = ", ".join(
                [
                    f"""
                    (
                        '{row["DATE"]}',
                        '{row["METRIC_NAME"]}',
                        '{row["TABLE"]}',
                        '{row[identifier_column]}',
                        {f"'{row['COLUMN']}'" if row["COLUMN"] is not None else "NULL"},
                        {row["VALUE"] if row["VALUE"] is not None else "NULL"},
                        {format_threshold(row["LOWER_THRESHOLD"]).format(numeric_type=DATA_TYPES["NUMERIC"][self.database_provider.type])},
                        {format_threshold(row["UPPER_THRESHOLD"]).format(numeric_type=DATA_TYPES["NUMERIC"][self.database_provider.type])},
                        '{row["RESULT"]}',
                        '{row["INSERT_TIMESTAMP"]}'
                    )
                """
                    for row in results_with_it
                ],
            )
            query_insert_values_into_result_table = f"""
                INSERT INTO {self.result_table}
                (DATE, METRIC_NAME, `TABLE`, {identifier_column}, `COLUMN`, VALUE, LOWER_THRESHOLD, UPPER_THRESHOLD, RESULT, INSERT_TIMESTAMP)
                VALUES {values_clause}
            """  # noqa: S608, E501
            try:
                execute_query(
                    query_insert_values_into_result_table,
                    self.duckdb_client,
                    self.config.database_accessor,
                    self.database_provider,
                )
            except duckdb.Error as e:
                msg = f"Could not insert rows into table {self.result_table}"
                raise DatabaseError(msg) from e

            accessor_prefix = f"{self.config.database_accessor}." if self.config.database_accessor else ""
            log.info(
                "%d entries were persisted to %s%s",
                len(results_with_it),
                accessor_prefix,
                self.result_table,
            )

    def __call__(self) -> list[dict]:
        """Execute all checks and return results."""
        self.execute_checks()
        log.info("Ran %d checks", len(self.checks))

        if self.check_failed:
            log.info(self.get_failed_checks_msg())

        if self.persist_results:
            self.load_to_database()

        if self.log_path:
            failed_checks_msg = self.get_failed_checks_msg()
            if not failed_checks_msg:
                log.info("No failed checks, so no log file was written.")
            else:
                with Path(self.log_path).open("w", encoding="utf-8") as file:
                    file.write(self.get_failed_checks_msg())
            log.info("DQM outputs were written to %s", self.log_path)

        return self.result_dicts
