import os
from unittest.mock import Mock

import pytest

from koality.checks import DataQualityCheck


class SuccessCheck(DataQualityCheck):
    """
    Check which is always successful.
    """

    def __init__(self, name_suffix, shop_id, **kwargs):
        super().__init__(
            table="dummy",
            **kwargs,
        )
        self.shop_id = shop_id
        self.transformation_name = f"success_check_{name_suffix}"

    def assemble_query(self):
        pass

    def assemble_data_exists_query(self):
        pass

    def assemble_name(self):
        pass

    def data_check(self, bq_client):
        return {}

    def check(self, bq_client):
        self.success = True
        self.status = "SUCCESS"
        self.bytes_billed = 10
        return {
            "DATE": "2023-09-18",
            "METRIC_NAME": self.transformation_name,
            "SHOP_ID": self.shop_id,
            "TABLE": "successful_table",
            "COLUMN": "column",
            "VALUE": 7,
            "LOWER_THRESHOLD": 0,
            "UPPER_THRESHOLD": 10,
            "RESULT": "SUCCESS",
        }


class FailureCheck(SuccessCheck):
    """
    Check which always fails.
    """

    def __init__(self, name_suffix, shop_id, **kwargs):
        super().__init__(
            name_suffix=name_suffix,
            shop_id=shop_id,
            **kwargs,
        )
        self.shop_id = shop_id
        self.transformation_name = f"failure_check_{name_suffix}"

    def check(self, duckdb_client):
        self.success = False
        self.status = "FAIL"
        self.bytes_billed = 11
        self.message = f"{self.transformation_name} failed badly!"
        return {
            "DATE": "2023-09-18",
            "METRIC_NAME": self.transformation_name,
            "SHOP_ID": self.shop_id,
            "TABLE": "failed_table",
            "COLUMN": "column",
            "VALUE": 77,
            "LOWER_THRESHOLD": 0,
            "UPPER_THRESHOLD": 10,
            "RESULT": "FAIL",
        }


class NoDataFailureCheck(FailureCheck):
    """
    Check which always fails due to missing data.
    """

    def __init__(self, empty_table, shop_id, date, **kwargs):
        super().__init__(
            name_suffix=empty_table,
            shop_id=shop_id,
            **kwargs,
        )
        self.empty_table = empty_table
        self.date = date

    def check(self, duckdb_client):
        self.success = False
        self.status = "FAIL"
        self.bytes_billed = 11
        self.message = f"No data in {self.empty_table} on {self.date} for: {self.shop_id}"
        return {
            "DATE": self.date,
            "METRIC_NAME": "data_exists",
            "SHOP_ID": self.shop_id,
            "TABLE": self.empty_table,
        }


class TestExecutorLogic:
    @pytest.fixture()
    def check_executor(self):
        from koality.executor import CheckExecutor

        return CheckExecutor

    @pytest.fixture
    def bq_client_mock(self):
        mock = Mock()
        mock.insert_rows.return_value = None
        return mock

    @pytest.fixture()
    def config_file_success(self, tmp_path):
        content = f"""
name: koality-all-success

global_defaults:
  monitor_only: False
  result_table: dataquality.data_koality_monitoring
  persist_results: True
  log_path: {os.path.abspath(tmp_path)}/message.txt

check_bundles:
  - name: check-bundle-1
    default_args:
      check_type: koality.tests.test_executor.SuccessCheck
    checks:
      - shop_id: SHOP001
        name_suffix: "1-1"
      - shop_id: SHOP002
        name_suffix: "1-2"

  - name: check-bundle-2
    default_args:
      check_type: koality.tests.test_executor.SuccessCheck
    checks:
      - shop_id: SHOP001
        name_suffix: "2-1"
      - shop_id: SHOP002
        name_suffix: "2-2"
"""
        tmp_file = tmp_path / "koality_config.yaml"
        tmp_file.write_text(content)
        return os.path.abspath(tmp_file)

    @pytest.fixture()
    def config_file_failure(self, tmp_path):
        content = f"""
name: koality-failure

global_defaults:
  monitor_only: False
  result_table: dataquality.data_koality_monitoring
  persist_results: True
  log_path: {os.path.abspath(tmp_path)}/message.txt

check_bundles:
  - name: check-bundle-1
    default_args:
      check_type: koality.tests.test_executor.SuccessCheck
    checks:
      - shop_id: SHOP001
        check_type: koality.tests.test_executor.SuccessCheck
        name_suffix: "1-1"
      - shop_id: SHOP002
        check_type: koality.tests.test_executor.FailureCheck
        name_suffix: "1-2"
"""
        tmp_file = tmp_path / "koality_config.yaml"
        tmp_file.write_text(content)
        return os.path.abspath(tmp_file)

    @pytest.fixture
    def config_file_missing_data_failure(self, tmp_path):
        content = f"""
name: koality-failure

global_defaults:
  monitor_only: False
  result_table: dataquality.data_koality_monitoring
  persist_results: True
  log_path: {os.path.abspath(tmp_path)}/message.txt
  date: "2023-09-18"

check_bundles:
  - name: check-bundle-1
    default_args:
      check_type: koality.tests.test_executor.NoDataFailureCheck
      empty_table: "test-project-prod.dataset_filtered.view_product_feed"
    checks:
      - shop_id: SHOP001
      - shop_id: SHOP004
      - shop_id: SHOP005
      - shop_id: SHOP011

  - name: check-bundle-2
    default_args:
      check_type: koality.tests.test_executor.NoDataFailureCheck
      empty_table: "test-project-prod.dataset_filtered.view_sku_feed"
    checks:
      - shop_id: SHOP802
      - shop_id: SHOP701

  # Failing regular checks, should not be aggregated
  - name: check-bundle-3
    default_args:
      check_type: koality.tests.test_executor.FailureCheck
    checks:
      - shop_id: SHOP601
        name_suffix: "3-1"
      - shop_id: SHOP502
        name_suffix: "3-2"
"""
        tmp_file = tmp_path / "koality_config.yaml"
        tmp_file.write_text(content)
        return os.path.abspath(tmp_file)

    def test_executor_all_success(self, config_file_success, check_executor, bq_client_mock):
        executor = check_executor(config_path=config_file_success, bq_client=bq_client_mock)
        result_dict = executor()
        result = {item["METRIC_NAME"] for item in result_dict}
        expected = {
            "success_check_1-1",
            "success_check_1-2",
            "success_check_2-1",
            "success_check_2-2",
        }

        assert result == expected
        assert os.path.getsize(os.path.join(os.path.dirname(config_file_success), "message.txt")) == 0
        assert executor.bytes_billed == 40
        assert executor.check_failed is False

        rows = bq_client_mock.insert_rows.call_args[1]["rows"]
        result = {item["METRIC_NAME"] for item in rows}
        assert result == expected

    def test_executor_failure(self, config_file_failure, check_executor, bq_client_mock):
        executor = check_executor(config_path=config_file_failure, bq_client=bq_client_mock)
        result_dict = executor()
        result = {item["METRIC_NAME"] for item in result_dict}
        expected = {"success_check_1-1", "failure_check_1-2"}

        assert result == expected

        message_file = os.path.join(os.path.dirname(config_file_failure), "message.txt")
        with open(file=message_file, mode="r") as f:
            check_message = f.read()

        assert check_message == "failure_check_1-2 failed badly!"
        assert executor.bytes_billed == 21
        assert executor.check_failed is True

        rows = bq_client_mock.insert_rows.call_args[1]["rows"]
        result = {item["METRIC_NAME"] for item in rows}
        assert result == expected

    def test_executor_missing_data(self, config_file_missing_data_failure, check_executor, bq_client_mock):
        executor = check_executor(config_path=config_file_missing_data_failure, bq_client=bq_client_mock)
        executor()

        message_file = os.path.join(os.path.dirname(config_file_missing_data_failure), "message.txt")
        with open(file=message_file, mode="r") as f:
            check_message = f.read()

        assert check_message == (
            "No data in test-project-prod.dataset_filtered.view_product_feed on 2023-09-18 for: "
            "SHOP001, SHOP004, SHOP005, SHOP011\n"
            "No data in test-project-prod.dataset_filtered.view_sku_feed on 2023-09-18 for: "
            "SHOP802, SHOP701\n"
            "failure_check_3-1 failed badly!\n"
            "failure_check_3-2 failed badly!"
        )

        expected_result_df = pd.DataFrame(
            {
                "DATE": "2023-09-18",
                "SHOP_ID": [
                    "SHOP001, SHOP004, SHOP005, SHOP011",
                    "SHOP802, SHOP701",
                    "SHOP601",
                    "SHOP502",
                ],
                "METRIC_NAME": ["data_exists"] * 2 + ["failure_check_3-1", "failure_check_3-2"],
                "TABLE": [
                    "test-project-prod.dataset_filtered.view_product_feed",
                    "test-project-prod.dataset_filtered.view_sku_feed",
                ]
                + ["failed_table"] * 2,
                "COLUMN": [None] * 2 + ["column"] * 2,
                "VALUE": [None] * 2 + [77] * 2,
                "LOWER_THRESHOLD": [None] * 2 + [0] * 2,
                "UPPER_THRESHOLD": [None] * 2 + [10] * 2,
                "RESULT": "FAIL",
            }
        )

        real_result_df = pd.DataFrame(executor._aggregate_result_dicts(executor.result_dicts))
        del real_result_df["INSERT_TIMESTAMP"]

        assert_frame_equal(real_result_df, expected_result_df)
        assert executor.bytes_billed == 88
        assert executor.check_failed is True
        rows = bq_client_mock.insert_rows.call_args[1]["rows"]
        first_row = rows[0]
        del first_row["INSERT_TIMESTAMP"]
        assert first_row in expected_result_df.replace(np.nan, None).to_dict(orient="records")
