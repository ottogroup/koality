"""Module containing the actual check execution logic."""

import logging
from typing import List
import duckdb

from koality.checks import DataQualityCheck, NullRatioCheck, RegexMatchCheck, ValuesInSetCheck, RollingValuesInSetCheck, DuplicateCheck, \
    CountCheck, OccurrenceCheck, MatchRateCheck, RelCountChangeCheck, IqrOutlierCheck
from koality.models import Config, CHECK_TYPE

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CHECK_MAP: dict[CHECK_TYPE, type[DataQualityCheck]] = {
    "NullRatioCheck": NullRatioCheck,
    "RegexMatchCheck": RegexMatchCheck,
    "ValuesInSetCheck": ValuesInSetCheck,
    "RollingValuesInSetCheck": RollingValuesInSetCheck,
    "DuplicateCheck": DuplicateCheck,
    "CountCheck": CountCheck,
    "OccurrenceCheck": OccurrenceCheck,
    "MatchRateCheck": MatchRateCheck,
    "RelCountChangeCheck": RelCountChangeCheck,
    "IqrOutlierCheck": IqrOutlierCheck
}


class CheckExecutor:
    """
    Provides all the logic to actually run checks. It contains methods to
    load configuration files, to execute checks, to create the failed
    checks log, and to export DQM results to a BQ table.

    Args:
        config_path: Path to koality configuration file
        bq_client: BigQuery client for interacting with BigQuery
        storage_client: Storage client for interacting with GCP buckets (optional)
    """

    def __init__(
        self,
        config: Config,
        **kwargs,
    ) -> None:
        self.config = config
        self.duckdb_client = duckdb.connect(":memory:")
        self.duckdb_client.query(self.config.database_setup)
        result = self.duckdb_client.execute(
            """
            SELECT COUNT(*) > 0 AS exists
            FROM duckdb_databases()
            WHERE database_name = 'bq'
            """
        ).fetchone()
        if result is None or result[0] is None:
            raise ValueError("DuckDB BigQuery extension is not loaded properly.")

        self.kwargs = kwargs

        self.checks: List = []
        self.check_failed = False

        self.jobs_: List = []

        self.result_dicts: List = []
        self.result_table = self.config.global_defaults.result_table
        self.persist_results = self.config.global_defaults.persist_results
        self.log_path = self.config.global_defaults.log_path

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

        check_suite_dict = self.config
        results = []
        for check_bundle in self.config.check_bundles:
            for check in check_bundle.checks:
                # check_with_default_args = check_bundle.get("default_args", {}) | check
                # log.info(", ".join(["{}: {}".format(k, v) for k, v in check_with_default_args.items()]))
                # check_dict = check_suite_dict.get("global_defaults", {}) | check_with_default_args
                check_factory = CHECK_MAP[check.check_type]
                check_kwargs = check.model_dump(exclude={"check_type"})
                # qualify tables with database accessor if provided
                if self.config.database_accessor:
                    for table_reference in ["table", "left_table", "right_table"]:
                        if table_reference in check_kwargs:
                            check_kwargs[table_reference] = f"{self.config.database_accessor}.{check_kwargs[table_reference]}"
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

        # Copy rows first, cause INSERT_TIMESTAMP and AUTO value is only a BQ feature and not needed anywhere else
        results_with_it: list[dict] = self._aggregate_result_dicts(self.result_dicts).copy()
        # Add INSERT_TIMESTAMP col with AUTO value to automatically set insert_timestamp (BQ feature)
        for row in results_with_it:
            row["INSERT_TIMESTAMP"] = "AUTO"

        # Convert results_with_it to VALUES clause
        values_clause = ", ".join([
            f"('{row['DATE']}', '{row['METRIC_NAME']}', '{row['TABLE']}', '{row['SHOP_ID']}', "
            f"'{row['COLUMN']}', '{row['VALUE']}', '{row['LOWER_THRESHOLD']}', "
            f"'{row['UPPER_THRESHOLD']}', '{row['RESULT']}', '{row['INSERT_TIMESTAMP']}')"
            for row in results_with_it
        ])
        if self.config.database_accessor:
            try:
                # make sure table exists
                self.duckdb_client.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.config.database_accessor}.{self.result_table} (DATE DATE, METRIC_NAME VARCHAR, "TABLE" VARCHAR, SHOP_ID VARCHAR,
                    "COLUMN" VARCHAR, VALUE VARCHAR, LOWER_THRESHOLD VARCHAR, UPPER_THRESHOLD VARCHAR, RESULT VARCHAR, INSERT_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
                    """
                )
                self.duckdb_client.execute(
                    f"INSERT INTO {self.config.database_accessor}.{self.result_table} "
                    f"SELECT * FROM VALUES({values_clause})"
                )
            except Exception as e:
                raise ValueError(f"response {e} could not be inserted into table")

            log.info(f"{len(results_with_it)} entries were persisted to {self.config.database_accessor}.{self.result_table}")

    def __call__(self):
        self.execute_checks()
        log.info(
            f"Ran {len(self.checks)} checks and used "
            # f"{round(self.bytes_billed / 2**30, 2)}GiB ({round(6 * self.bytes_billed / 2**40, 2)}€)."
        )

        if self.check_failed:
            log.info(self.get_failed_checks_msg())

        if self.persist_results:
            self.load_to_database()

        if self.log_path:
            with open(self.log_path, "w", encoding="utf-8") as file:
                file.write(self.get_failed_checks_msg())
            log.info(f"DQM outputs were written to {self.log_path}")

        return self.result_dicts
