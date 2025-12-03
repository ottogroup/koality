from unittest.mock import Mock

import pytest


def create_bq_client(check_name: str = "row_count", check_value: float = 99.0, empty_table: str = ""):
    bq_client = Mock()
    bq_client.query = Mock()
    bq_client.project = "test-project-dev"

    query_job_check_data = Mock()
    query_job_check_data.state = "DONE"
    query_job_check_data.total_bytes_billed = 1024 * 1024 * 1024
    query_job_check_data.result.return_value = [{"empty_table": empty_table}]

    query_job_check = Mock()
    query_job_check.state = "DONE"
    query_job_check.total_bytes_billed = 1024 * 1024 * 1024
    query_job_check.result.return_value = [{check_name: check_value}]
    bq_client.query.side_effect = [query_job_check_data, query_job_check]

    return bq_client


class TestChecks:
    @pytest.fixture()
    def count_check(self):
        from koality.checks import CountCheck

        return CountCheck

    def test_message_no_extra_info(self, count_check):
        bq_client = create_bq_client("row_count", 99)

        check = count_check(
            date="2023-01-01",
            table="dummy_table",
            shop_id="SHOP001",
            check_column="*",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            lower_threshold=1000,
            upper_threshold=9999,
        )
        check(bq_client)

        assert check.message == (
            "SHOP001: Metric row_count failed on 2023-01-01 for dummy_table. Value 99 is not between 1000 and 9999."
        )

    def test_message_date_info(self, count_check):
        bq_client = create_bq_client("row_count", 99)

        check = count_check(
            date="2023-01-01",
            table="dummy_table",
            shop_id="SHOP001",
            check_column="*",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            lower_threshold=1000,
            upper_threshold=9999,
            date_info="PREDICTION_DATE = real date + 1",
        )
        check(bq_client)

        assert check.message == (
            "SHOP001: Metric row_count failed on 2023-01-01 "
            "(PREDICTION_DATE = real date + 1) for dummy_table. Value 99 is not between 1000 and 9999."
        )

    def test_message_extra_info(self, count_check):
        bq_client = create_bq_client("row_count", 99)

        check = count_check(
            date="2023-01-01",
            table="dummy_table",
            shop_id="SHOP001",
            check_column="*",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            lower_threshold=1000,
            upper_threshold=9999,
            extra_info="Note: This is an awesome check.",
        )
        check(bq_client)

        assert check.message == (
            "SHOP001: Metric row_count failed on 2023-01-01 for dummy_table. "
            "Value 99 is not between 1000 and 9999. Note: This is an awesome check."
        )

    def test_message_correct_formatting(self, count_check):
        """
        Test checks if output still returns a useful formatted value even if
        rounding by min_precision would lead to zero.
        """

        bq_client = create_bq_client("row_count", 0.0000123)

        check = count_check(
            date="2023-01-01",
            table="dummy_table",
            shop_id="SHOP001",
            check_column="*",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            lower_threshold=0,
            upper_threshold=0,
        )
        check(bq_client)

        assert check.message == (
            "SHOP001: Metric row_count failed on 2023-01-01 for dummy_table. Value 0.00001 is not between 0 and 0."
        )
