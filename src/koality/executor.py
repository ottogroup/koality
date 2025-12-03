"""Module containing the actual check execution logic."""

import logging
from concurrent import futures
from typing import List, Optional

import pandas as pd
import yaml
from google.cloud import bigquery as bq

# Workaround for typing: https://github.com/python/mypy/issues/12985
from google.cloud.storage import Client as StorageClient

from koality.utils import resolve_dotted_name

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


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
        config_path: str,
        bq_client: bq.Client,
        storage_client: Optional[StorageClient] = None,
        **kwargs,
    ) -> None:
        self.config_path = config_path
        self.bq_client = bq_client
        self.storage_client = storage_client

        self.kwargs = kwargs
        self.config = self.read_check_config()

        self.checks: List = []
        self.check_failed = False

        self.jobs_: List = []

        self.result_dicts: List = []
        self.result_table = self.config.get("global_defaults", {}).get("result_table", None)
        self.persist_results = self.config.get("global_defaults", {}).get("persist_results", False)
        self.log_path = self.config.get("global_defaults", {}).get("log_path", False)

    @staticmethod
    def aggregate_values(value_list) -> str:
        return ", ".join(sorted(set(value_list)))

    def read_check_config(self) -> dict:
        """read check config"""
        with open(self.config_path, "r", encoding="utf-8") as f:
            check_bundle_dict = yaml.safe_load(f.read())

        # overwrite global defaults using kwargs
        check_bundle_dict["global_defaults"] = check_bundle_dict["global_defaults"] | self.kwargs

        return check_bundle_dict

    def execute_checks(self):
        """
        Instantiates and executes all checks using a `ThreadPoolExecutor`. When walking
        through the different checks, parameters are updated using global defaults,
        check defaults of bundles, and a check's parameters. Check results are stored
        in a results dict for further processing.
        """

        check_suite_dict = self.config
        self.jobs_ = []
        with futures.ThreadPoolExecutor() as exe:
            for check_bundle in check_suite_dict["check_bundles"]:
                for check in check_bundle["checks"]:
                    check_with_default_args = check_bundle.get("default_args", {}) | check
                    log.info(", ".join(["{}: {}".format(k, v) for k, v in check_with_default_args.items()]))

                    check_dict = check_suite_dict.get("global_defaults", {}) | check_with_default_args
                    check_factory = resolve_dotted_name(check_dict["check_type"])
                    check = check_factory(**check_dict)
                    self.checks.append(check)

                    self.jobs_.append(exe.submit(check, self.bq_client))

        # wait for jobs to finish
        completed_jobs = futures.wait(self.jobs_).done

        for check in self.checks:
            if check.status in ("FAIL", "ERROR"):
                self.check_failed = True
                break

        self.result_dicts = [job.result() for job in completed_jobs]

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
        df_msgs = pd.DataFrame({"orig_msg": msgs_no_data})
        df_msgs["table_part"] = df_msgs.orig_msg.map(lambda x: x.split(":")[0])
        df_msgs["shop_id"] = df_msgs.orig_msg.map(lambda x: x.split(":")[1].strip())
        df_grouped_shops = (
            df_msgs[["table_part", "shop_id"]].groupby("table_part").agg(self.aggregate_values).reset_index()
        )

        msgs_no_data = [f"{table_part}: {shop_id}" for _, table_part, shop_id in df_grouped_shops.itertuples()]

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
        if result_no_data:
            df_results = pd.DataFrame(result_no_data)
            df_grouped_shops = (
                df_results.groupby(["DATE", "METRIC_NAME", "TABLE"]).agg(self.aggregate_values).reset_index()
            )
            df_grouped_shops["COLUMN"] = None
            df_grouped_shops["VALUE"] = None
            df_grouped_shops["LOWER_THRESHOLD"] = None
            df_grouped_shops["UPPER_THRESHOLD"] = None
            df_grouped_shops["RESULT"] = "FAIL"
            result_no_data = df_grouped_shops.to_dict(orient="records")

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

    def load_to_bq(self) -> None:
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

        response = self.bq_client.insert_rows(
            table=self.bq_client.get_table(table=self.result_table),
            rows=results_with_it,
        )

        if response:
            raise ValueError(f"response {response} could not be inserted into table")

        log.info(f"{len(results_with_it)} entries were persisted to bq {self.result_table}")

    def __call__(self):
        self.execute_checks()
        log.info(
            f"Ran {len(self.checks)} checks and used "
            f"{round(self.bytes_billed / 2**30, 2)}GiB ({round(6 * self.bytes_billed / 2**40, 2)}€)."
        )

        if self.check_failed:
            log.info(self.get_failed_checks_msg())

        if self.persist_results:
            self.load_to_bq()

        if self.log_path:
            with open(self.log_path, "w", encoding="utf-8") as file:
                file.write(self.get_failed_checks_msg())
            log.info(f"DQM outputs were written to {self.log_path}")

        return self.result_dicts
