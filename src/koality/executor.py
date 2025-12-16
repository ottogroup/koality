"""Module containing the actual check execution logic."""

import datetime
import logging
from collections import defaultdict
from typing import List

import duckdb

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
from koality.utils import execute_query, identify_database_provider

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
    """
    Provides all the logic to actually run checks. It contains methods to
    execute checks, to create the failed checks log, and to export DQM
    results to a database table.

    Args:
        config: Koality configuration object
        duckdb_client: DuckDB client for interacting with DuckDB (optional).
            If not provided, an in-memory connection will be created.
    """

    def __init__(
        self,
        config: Config,
        duckdb_client: duckdb.DuckDBPyConnection | None = None,
        **kwargs,
    ) -> None:
        self.config = config
        if duckdb_client is not None:
            self.duckdb_client = duckdb_client
        else:
            self.duckdb_client = duckdb.connect(":memory:")
            self.duckdb_client.query(self.config.database_setup)

        self.kwargs = kwargs

        self.checks: List = []
        self.check_failed = False

        self.jobs_: List = []

        self.result_dicts: List = []
        self.result_table = self.config.defaults.result_table
        self.persist_results = self.config.defaults.persist_results
        self.log_path = self.config.defaults.log_path

    @staticmethod
    def aggregate_values(value_list) -> str:
        return ", ".join(sorted(set(value_list)))

    def execute_checks(self):
        """
        Instantiates and executes all checks using a `ThreadPoolExecutor`. When walking
        through the different checks, parameters are updated using global defaults,
        check defaults of bundles, and a check's parameters. Check results are stored
        in a results dict for further processing.
        """

        results = []
        database_provider = None
        if self.config.database_accessor:
            database_provider = identify_database_provider(self.duckdb_client, self.config.database_accessor)
        for check_bundle in self.config.check_bundles:
            for check in check_bundle.checks:
                check_factory = CHECK_MAP[check.check_type]
                check_kwargs = check.model_dump(exclude={"check_type"}, exclude_unset=True)
                check_kwargs["database_accessor"] = self.config.database_accessor
                check_kwargs["database_provider"] = database_provider
                check = check_factory(**check_kwargs)
                self.checks.append(check)

                results.append(check(self.duckdb_client))

        for check in self.checks:
            if check.status in ("FAIL", "ERROR"):
                self.check_failed = True
                break

        self.result_dicts = results

    def _aggregate_checks_msgs(self, msg_list: list[str]) -> list[str]:
        """
        Aggregates a list of (failure) check messages. The reason behind is
        that if data tables to be checked do not contain any data,
        a specific failure will be created. As a larger number of such
        failures can be created (for different checks or for different
        shop IDs), we aggregate missing data failures, grouping them
        by table and date and joining all distinct shop IDs to a
        comma-separated list.

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
        for msg in msgs_no_data:
            parts = msg.split(":", 1)
            if len(parts) == 2:
                table_part = parts[0]
                shop_id = parts[1].strip()
                if table_part not in grouped_data:
                    grouped_data[table_part] = []
                grouped_data[table_part].append(shop_id)

        msgs_no_data = [
            f"{table_part}: {self.aggregate_values(shop_ids)}" for table_part, shop_ids in grouped_data.items()
        ]

        return msgs_no_data + msgs_other

    def _aggregate_result_dicts(self, result_dicts: list[dict]) -> list[dict]:
        """
        Aggregates a list of check result dicts. The reason behind is
        that if data tables to be checked do not contain any data,
        a specific failure will be created. As a larger number of such
        failures can be created (for different checks or for different
        shop IDs), we aggregate missing data failures, grouping them
        by table and date and joining all distinct shop IDs to a
        comma-separated list.

        Args:
            result_dicts: A list of check result dicts.

        Returns:
            A list of check result dicts with aggregated missing data results.
        """

        # Other results to be left untouched
        result_other = [result for result in result_dicts if result["METRIC_NAME"] != "data_exists"]

        # Missing data results to be aggregated
        result_no_data = [result for result in result_dicts if result["METRIC_NAME"] == "data_exists"]

        # Group and aggregate messages
        if not result_no_data:
            return result_other

        grouped_data = {}
        for result in result_no_data:
            key = (result["DATE"], result["METRIC_NAME"], result["TABLE"])
            if key not in grouped_data:
                grouped_data[key] = {
                    "DATE": result["DATE"],
                    "METRIC_NAME": result["METRIC_NAME"],
                    "TABLE": result["TABLE"],
                    "SHOP_ID": [],
                }
            grouped_data[key]["SHOP_ID"].append(result["SHOP_ID"])

        result_no_data = [
            {
                "DATE": value["DATE"],
                "METRIC_NAME": value["METRIC_NAME"],
                "TABLE": value["TABLE"],
                "SHOP_ID": self.aggregate_values(value["SHOP_ID"]),
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
        """
        Get an aggregated message for all failed checks. Uses the message attribute of
        all checks and aggregates it.

        Returns:
            aggregated, sorted, newline separated messages of failed checks
        """

        failed_checks_msgs = [check.message for check in self.checks if check.message]
        failed_checks_msgs = self._aggregate_checks_msgs(failed_checks_msgs)
        failed_checks_msgs.sort()

        return "\n".join(failed_checks_msgs)

    def load_to_database(self) -> None:
        """
        Persists koality's DQM results in a BQ table. The result table is partitioned by
        DATE. DQM data is always appended to table.
        """

        if self.result_table is None:
            log.info("result_table is None. Results were not persisted.")
            return

        if len(self.result_dicts) == 0:
            log.info("No entries in results from checks, so no results were persisted.")
            return

        now = datetime.datetime.now()

        # Copy rows first, cause INSERT_TIMESTAMP and AUTO value is only a BQ feature and not needed anywhere else
        results_with_it: list[dict] = self._aggregate_result_dicts(self.result_dicts).copy()
        # Add INSERT_TIMESTAMP col with AUTO value to automatically set insert_timestamp (BQ feature)
        for row in results_with_it:
            row["INSERT_TIMESTAMP"] = now

        if self.config.database_accessor:
            database_provider = identify_database_provider(self.duckdb_client, self.config.database_accessor)
            query_create_or_replace_table = f"""
                CREATE TABLE IF NOT EXISTS {self.result_table} (
                    DATE {DATA_TYPES["DATE"][database_provider.type]},
                    METRIC_NAME {DATA_TYPES["VARCHAR"][database_provider.type]},
                    TABLE {DATA_TYPES["VARCHAR"][database_provider.type]},
                    SHOP_ID {DATA_TYPES["VARCHAR"][database_provider.type]},
                    COLUMN {DATA_TYPES["VARCHAR"][database_provider.type]},
                    VALUE {DATA_TYPES["VARCHAR"][database_provider.type]},
                    LOWER_THRESHOLD {DATA_TYPES["NUMERIC"][database_provider.type]},
                    UPPER_THRESHOLD {DATA_TYPES["NUMERIC"][database_provider.type]},
                    RESULT {DATA_TYPES["VARCHAR"][database_provider.type]},
                    INSERT_TIMESTAMP {DATA_TYPES["TIMESTAMP"][database_provider.type]} DEFAULT CURRENT_TIMESTAMP
                )
            """
            try:
                # make sure table exists
                execute_query(
                    query_create_or_replace_table,
                    self.duckdb_client,
                    database_provider,
                )
            except duckdb.Error as e:
                raise DatabaseError(f"Could not create or replace table {self.result_table}") from e

            # Convert results_with_it to VALUES clause
            values_clause = ", ".join(
                [
                    f"""
                    (
                        "{row["DATE"]}",
                        "{row["METRIC_NAME"]}",
                        "{row["TABLE"]}",
                        "{row["SHOP_ID"]}",
                        "{row["COLUMN"]}",
                        {row["VALUE"] if row["VALUE"] is not None else "NULL"},
                        {row["LOWER_THRESHOLD"]},
                        {row["UPPER_THRESHOLD"]},
                        "{row["RESULT"]}",
                        "{row["INSERT_TIMESTAMP"]}"
                    )
                """
                    for row in results_with_it
                ]
            )
            query_insert_values_into_result_table = f"""
                INSERT INTO {self.result_table}
                (DATE, METRIC_NAME, `TABLE`, SHOP_ID, `COLUMN`, VALUE, LOWER_THRESHOLD, UPPER_THRESHOLD, RESULT, INSERT_TIMESTAMP)
                VALUES {values_clause}
            """  # noqa: S608, E501
            try:
                execute_query(
                    query_insert_values_into_result_table,
                    self.duckdb_client,
                    database_provider,
                )
            except duckdb.Error as e:
                raise DatabaseError(f"Could not insert rows into table {self.result_table}") from e

            log.info(
                f"{len(results_with_it)} entries were persisted to "
                f"{f'{self.config.database_accessor}.' if self.config.database_accessor else ''}{self.result_table}"
            )

    def __call__(self):
        self.execute_checks()
        log.info(f"Ran {len(self.checks)} checks")

        if self.check_failed:
            log.info(self.get_failed_checks_msg())

        if self.persist_results:
            self.load_to_database()

        if self.log_path:
            failed_checks_msg = self.get_failed_checks_msg()
            if not failed_checks_msg:
                log.info("No failed checks, so no log file was written.")
            else:
                with open(self.log_path, "w", encoding="utf-8") as file:
                    file.write(self.get_failed_checks_msg())
            log.info(f"DQM outputs were written to {self.log_path}")

        return self.result_dicts
