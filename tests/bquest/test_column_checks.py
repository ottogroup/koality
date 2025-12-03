import datetime as dt

import numpy as np
import pandas as pd
import pytest
from bquest.dataframe import assert_frame_equal
from bquest.runner import SQLRunner
from bquest.tables import BQTableDefinitionBuilder
from google.cloud import bigquery as bq


@pytest.mark.bquest
class TestColumnChecks:
    @classmethod
    def setup_class(cls):
        bq_client = bq.Client(project="test-project-dev")
        cls.bq_client = bq_client
        cls.table_builder = BQTableDefinitionBuilder("test-project-dev")
        cls.runner = SQLRunner(bq_client)

    @pytest.fixture(scope="class")
    def dummy_table(self):
        df = pd.DataFrame(
            {
                "DATE": [pd.Timestamp("2023-01-01", tz="UTC")] * 7 + [pd.Timestamp("2023-01-15", tz="UTC")] * 4,
                "shop_name": ["shop-a.example"] * 4
                + [
                    "shop-b.example",
                    "shop-c.example",
                    "shop-d.example",
                ]
                + ["shop-a.example"] * 4,
                "shop_code": ["SHOP001"] * 4 + ["SHOP023", "SHOP002", "SHOP006"] + ["SHOP001"] * 4,
                "product_number": [
                    "SHOP001-0001",
                    "SHOP001-0002",
                    "SHOP001-0003",
                    "SHOP001-0040",
                    "SHOP023-0001",
                    "SHOP002-0001",
                    "SHOP006-0001",
                    "SHOP001-0001",
                    "SHOP001-0002",
                    "SHOP001-0003",
                    "SHOP001-0040",
                ],
                "num_orders": [5, 3, np.nan, 0] + [5, 1200, np.nan] + [11, 12, 13, 14],
                "assortment": ["toys", "toys", "furniture", "clothing"]
                + ["clothing", "clothing", "appliances"]
                + ["toys", "toys", "toys", "toys"],
            }
        )

        return self.table_builder.from_df(name="dataset.dummy_table", df=df)

    @pytest.fixture()
    def null_ratio_check(self):
        from koality.checks import NullRatioCheck

        return NullRatioCheck

    def test_null_ratio_check(self, null_ratio_check, dummy_table):
        # build check
        check = null_ratio_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="num_orders",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "num_orders_null_ratio": [
                    0.25,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_null_ratio_check_empty_table(self, null_ratio_check, dummy_table):
        # build check
        check = null_ratio_check(
            date="2023-01-02",  # not in dummy_table
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="num_orders",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "num_orders_null_ratio": [
                    np.nan,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.fixture()
    def regex_match_check(self):
        from koality.checks import RegexMatchCheck

        return RegexMatchCheck

    def test_regex_match_check_all_matched(self, regex_match_check, dummy_table):
        # match all product numbers
        check = regex_match_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="product_number",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            regex_to_match=r"SHOP001-\\d\\d\\d\\d",  # double escaped: r for python, \\ for bq
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_regex_match_ratio": [
                    1.0,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_regex_match_check_with_unmatched(self, regex_match_check, dummy_table):
        # check if one product number does not match
        # match "SHOP001-0001" but not "SHOP001-0020"
        check = regex_match_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="product_number",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            regex_to_match=r"SHOP001-000\\d",  # double escaped: r for python, \\ for bq
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_regex_match_ratio": [
                    0.75,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.fixture()
    def values_in_set_check(self):
        from koality.checks import ValuesInSetCheck

        return ValuesInSetCheck

    def test_values_in_set_check_value_given(self, values_in_set_check, dummy_table):
        """
        Simple test case with 3/4 entries where values in value set are given.
        """

        check = values_in_set_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            value_set='("toys", "furniture")',
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_values_in_set_ratio": [
                    0.75,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_values_in_set_check_value_not_given(self, values_in_set_check, dummy_table):
        """
        Test case with data for shop/day combination, but no occurrences of values
        in value set.
        """

        check = values_in_set_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            value_set='("weird non-existing value", "another weird value")',
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_values_in_set_ratio": [
                    0.0,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.mark.parametrize(
        "day,shop",
        [
            ("2023-01-01", "SHOP999"),
            ("2022-12-31", "SHOP001"),
            ("2023-01-02", "SHOP001"),
            ("2023-01-02", "SHOP999"),
        ],
    )
    def test_values_in_set_check_no_data(self, values_in_set_check, dummy_table, day, shop):
        """
        Test check if there is no data for the shop / day combination.
        """

        check = values_in_set_check(
            date=day,
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id=shop,
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            value_set='("toys", "furniture")',
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_values_in_set_ratio": [
                    np.nan,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_values_in_set_check_value_single_value(self, values_in_set_check, dummy_table):
        """
        Simple test case with 3/4 entries where values in value set are given.
        """

        check = values_in_set_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            value_set='"toys"',
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_values_in_set_ratio": [
                    0.5,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.fixture()
    def rolling_values_in_set_check(self):
        from koality.checks import RollingValuesInSetCheck

        return RollingValuesInSetCheck

    def test_rolling_values_in_set_check_value_given_single_day(self, rolling_values_in_set_check, dummy_table):
        """
        Test case with 1 day data with 2/4 entries where values in value set are given.
        """

        check = rolling_values_in_set_check(
            date="2023-01-14",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            value_set='("toys")',
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_rolling_values_in_set_ratio": [
                    0.5,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_rolling_values_in_set_check_value_given_2_days(self, rolling_values_in_set_check, dummy_table):
        """
        Test case with 2 days data with 6/8 entries where values in value set are given.
        """

        check = rolling_values_in_set_check(
            date="2023-01-15",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
            value_set='("toys")',
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_rolling_values_in_set_ratio": [
                    0.75,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.fixture()
    def duplicate_check(self):
        from koality.checks import DuplicateCheck

        return DuplicateCheck

    def test_duplicate_check_duplicates(self, duplicate_check, dummy_table):
        """
        Simple duplicate check using assortment column for SHOP001
        which contains 3 distinct values, thus, 4 - 3 = 1 duplicates occur.
        """

        check = duplicate_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_duplicates": [
                    1,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_duplicate_check_no_duplicates(self, duplicate_check, dummy_table):
        """
        Simple duplicate check using assortment column for SHOP023
        where no duplicates occur.
        """

        check = duplicate_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP023",
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_duplicates": [
                    0,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_duplicate_check_no_data(self, duplicate_check, dummy_table):
        """
        Simple duplicate check using assortment column for a non-existing
        shop_id (SHOP999).
        """

        check = duplicate_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP999",  # no data for this shop
            check_column="assortment",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_duplicates": [
                    0,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.fixture()
    def count_check(self):
        from koality.checks import CountCheck

        return CountCheck

    def test_count_check_regular(self, count_check, dummy_table):
        """
        Test simple case of counting all rows.
        """

        check = count_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="*",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "row_count": [
                    4,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    def test_count_check_distinct_column(self, count_check, dummy_table):
        """
        Test simple case of counting all distinct values of a column.
        """

        check = count_check(
            date="2023-01-01",
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id="SHOP001",
            check_column="assortment",
            distinct=True,
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "assortment_distinct_count": [
                    3,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.mark.parametrize(
        "day,shop",
        [
            ("2023-01-01", "SHOP999"),
            ("2022-12-31", "SHOP001"),
            ("2023-01-02", "SHOP001"),
            ("2023-01-02", "SHOP999"),
        ],
    )
    def test_count_check_regular_no_data(self, count_check, dummy_table, day, shop):
        """
        Test check if there is no data for the shop / day combination.
        """

        check = count_check(
            date=day,
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            shop_id=shop,
            check_column="*",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "row_count": [
                    0,
                ]
            }
        )
        assert_frame_equal(expected_df, result_df)

    @pytest.fixture()
    def occurence_check(self):
        from koality.checks import OccurenceCheck

        return OccurenceCheck

    @pytest.mark.parametrize(
        "max_or_min, lower_threshold, upper_threshold, product_number, result_value, result_code",
        [("min", 0, 2, "SHOP023-0001", 1, "SUCCESS"), ("max", 0, 1, "SHOP001-0001", 2, "FAIL")],
    )
    def test_occurence_check(
        self,
        occurence_check,
        dummy_table,
        max_or_min,
        lower_threshold,
        upper_threshold,
        product_number,
        result_value,
        result_code,
    ):
        """
        Test whether any item occurs more / less often than specified
        """

        check = occurence_check(
            max_or_min=max_or_min,
            table=self.table_builder._dataset + "." + dummy_table.table_name,
            check_column="product_number",
            lower_threshold=lower_threshold,
            upper_threshold=upper_threshold,
        )
        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number": [product_number],
                f"product_number_occurence_{max_or_min}": [result_value],
            }
        )
        assert_frame_equal(expected_df, result_df)

        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "product_number",
            "METRIC_NAME": f"product_number_occurence_{max_or_min}",
            "TABLE": self.table_builder._dataset + "." + dummy_table.table_name,
            "DATE": dt.date.today().isoformat(),
            "VALUE": result_value,
            "LOWER_THRESHOLD": lower_threshold,
            "UPPER_THRESHOLD": upper_threshold,
            "RESULT": result_code,
            "SHOP_ID": "ALL_SHOPS",
        }

    def test_occurence_check_faulty_mode(self, occurence_check, dummy_table):
        """
        Test faulty mode for occurence check
        """
        with pytest.raises(ValueError) as exec_info:
            occurence_check(
                max_or_min="foo",
                table=self.table_builder._dataset + "." + dummy_table.table_name,
                check_column="product_number",
            )
        assert exec_info.match("supported modes 'min' or 'max'")

    @pytest.fixture
    def iqr_outlier_check(self):
        from koality.checks import IqrOutlierCheck

        return IqrOutlierCheck

    @pytest.fixture(scope="class")
    def dummy_table_iqr(self):
        df = pd.DataFrame(
            {
                "BQ_PARTITIONTIME": pd.date_range("2023-01-01", "2023-01-15", tz="UTC"),
                "VALUE": [1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 101, 102, 101, 102, 101],
            }
        )

        return self.table_builder.from_df(name="dataset.dummy_table_iqr", df=df)

    @pytest.fixture(scope="class")
    def dummy_table_iqr_two_shops(self):
        df = pd.DataFrame(
            {
                "BQ_PARTITIONTIME": [
                    *pd.date_range("2023-01-01", "2023-01-15", tz="UTC"),
                    *pd.date_range("2023-01-01", "2023-01-15", tz="UTC"),
                ],
                "VALUE": [1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 101, 102, 101, 102, 101]
                + [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 101, 102, 101, 102, 101],
                "SHOP_ID": ["abcd"] * 15 + ["efgh"] * 15,
            }
        )

        return self.table_builder.from_df(name="dataset.dummy_table_iqr_two_shops", df=df)

    @pytest.fixture(scope="class")
    def dummy_table_iqr_oven(self):
        # make sure the oven gate case would have been detected
        df = pd.DataFrame(
            {
                "BQ_PARTITIONTIME": [
                    pd.Timestamp("2024-02-01", tz="UTC"),
                    pd.Timestamp("2024-02-02", tz="UTC"),
                    pd.Timestamp("2024-02-04", tz="UTC"),
                    pd.Timestamp("2024-02-05", tz="UTC"),
                    pd.Timestamp("2024-02-06", tz="UTC"),
                    pd.Timestamp("2024-02-07", tz="UTC"),
                    pd.Timestamp("2024-02-08", tz="UTC"),
                    pd.Timestamp("2024-02-09", tz="UTC"),
                    pd.Timestamp("2024-02-11", tz="UTC"),
                    pd.Timestamp("2024-02-12", tz="UTC"),
                    pd.Timestamp("2024-02-13", tz="UTC"),
                    pd.Timestamp("2024-02-14", tz="UTC"),
                    pd.Timestamp("2024-02-15", tz="UTC"),
                ],
                "VALUE": [
                    53,
                    41,
                    71,
                    57,
                    24,
                    46,
                    38,
                    35,
                    33,
                    554,
                    583,
                    47,
                    32,
                ],
            }
        )

        return self.table_builder.from_df(name="dataset.dummy_table_iqr", df=df)

    @pytest.fixture(scope="class")
    def dummy_table_iqr_latest_value_missing(self):
        df = pd.DataFrame(
            {
                "BQ_PARTITIONTIME": pd.date_range("2023-01-01", "2023-01-15", tz="UTC"),
                "VALUE": [1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 101, 102, 101, 102, None],
            }
        )

        return self.table_builder.from_df(name="dataset.dummy_table_iqr_latest_value_missing", df=df)

    def test_iqr_outlier_check_success(self, iqr_outlier_check, dummy_table_iqr):
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr.table_name,
            date="2023-01-15",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="both",
            iqr_factor=1.5,
        )
        _ = self.runner.run(
            check.query,
            [dummy_table_iqr],
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "VALUE",
            "METRIC_NAME": "VALUE_outlier_iqr_both_1_5",
            "TABLE": self.table_builder._dataset + "." + dummy_table_iqr.table_name,
            "DATE": "2023-01-15",
            "VALUE": 101.0,
            "LOWER_THRESHOLD": -111.875,
            "UPPER_THRESHOLD": 189.125,
            "RESULT": "SUCCESS",
            "SHOP_ID": "ALL_SHOPS",
        }

    def test_iqr_outlier_check_two_shops_success(self, iqr_outlier_check, dummy_table_iqr_two_shops):
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr_two_shops.table_name,
            date="2023-01-15",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="both",
            iqr_factor=1.5,
            shop_id="abcd",
            shop_id_filter_column="SHOP_ID",
        )
        _ = self.runner.run(
            check.query,
            [dummy_table_iqr_two_shops],
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "VALUE",
            "METRIC_NAME": "VALUE_outlier_iqr_both_1_5",
            "TABLE": self.table_builder._dataset + "." + dummy_table_iqr_two_shops.table_name,
            "DATE": "2023-01-15",
            "VALUE": 101.0,
            "LOWER_THRESHOLD": -111.875,
            "UPPER_THRESHOLD": 189.125,
            "RESULT": "SUCCESS",
            "SHOP_ID": "abcd",
        }
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr_two_shops.table_name,
            date="2023-01-15",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="both",
            iqr_factor=1.5,
            shop_id="efgh",
            shop_id_filter_column="SHOP_ID",
        )
        _ = self.runner.run(
            check.query,
            [dummy_table_iqr_two_shops],
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "VALUE",
            "METRIC_NAME": "VALUE_outlier_iqr_both_1_5",
            "TABLE": self.table_builder._dataset + "." + dummy_table_iqr_two_shops.table_name,
            "DATE": "2023-01-15",
            "VALUE": 101.0,
            "LOWER_THRESHOLD": -106.75,
            "UPPER_THRESHOLD": 189.25,
            "RESULT": "SUCCESS",
            "SHOP_ID": "efgh",
        }

    def test_iqr_outlier_check_failure(self, iqr_outlier_check, dummy_table_iqr):
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr.table_name,
            date="2023-01-11",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="both",
            iqr_factor=1.5,
        )
        _ = self.runner.run(
            check.query,
            [dummy_table_iqr],
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "VALUE",
            "METRIC_NAME": "VALUE_outlier_iqr_both_1_5",
            "TABLE": self.table_builder._dataset + "." + dummy_table_iqr.table_name,
            "DATE": "2023-01-11",
            "VALUE": 101.0,
            "LOWER_THRESHOLD": -0.5,
            "UPPER_THRESHOLD": 3.5,
            "RESULT": "FAIL",
            "SHOP_ID": "ALL_SHOPS",
        }

    def test_iqr_outlier_check_success_because_only_lower(self, iqr_outlier_check, dummy_table_iqr):
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr.table_name,
            date="2023-01-11",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="lower",
            iqr_factor=1.5,
        )
        _ = self.runner.run(
            check.query,
            [dummy_table_iqr],
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "VALUE",
            "METRIC_NAME": "VALUE_outlier_iqr_lower_1_5",
            "TABLE": self.table_builder._dataset + "." + dummy_table_iqr.table_name,
            "DATE": "2023-01-11",
            "VALUE": 101.0,
            "LOWER_THRESHOLD": -0.5,
            "UPPER_THRESHOLD": np.inf,
            "RESULT": "SUCCESS",
            "SHOP_ID": "ALL_SHOPS",
        }

    @pytest.mark.parametrize(
        "option",
        [
            {"interval_days": 0},
            {"how": "foo"},
            {"iqr_factor": 1.4},
        ],
    )
    def test_iqr_outlier_check_value_error(self, iqr_outlier_check, dummy_table_iqr, option):
        kwargs = {
            "check_column": "VALUE",
            "table": self.table_builder._dataset + "." + dummy_table_iqr.table_name,
            "date": "2023-01-11",
            "date_filter_column": "BQ_PARTITIONTIME",
            "interval_days": 14,
            "how": "lower",
            "iqr_factor": 1.5,
        } | option
        with pytest.raises(ValueError):
            iqr_outlier_check(**kwargs)

    def test_iqr_outlier_check_data_exists_error(self, iqr_outlier_check, dummy_table_iqr_latest_value_missing):
        dummy_table_iqr_latest_value_missing.load_to_bq(self.bq_client)
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr_latest_value_missing.table_name,
            date="2023-01-15",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="upper",
            iqr_factor=1.5,
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "DATE": "2023-01-15",
            "METRIC_NAME": "data_exists",
            "SHOP_ID": "ALL_SHOPS",
            "TABLE": f"{dummy_table_iqr_latest_value_missing.dataset}.{dummy_table_iqr_latest_value_missing.table_name}",  # noqa: E501
        }

    def test_iqr_outlier_check_failure_oven_2024_02_12(self, iqr_outlier_check, dummy_table_iqr_oven):
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr_oven.table_name,
            date="2024-02-12",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="upper",
            iqr_factor=1.5,
        )
        _ = self.runner.run(
            check.query,
            [dummy_table_iqr_oven],
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "VALUE",
            "METRIC_NAME": "VALUE_outlier_iqr_upper_1_5",
            "TABLE": self.table_builder._dataset + "." + dummy_table_iqr_oven.table_name,
            "DATE": "2024-02-12",
            "VALUE": 554.0,
            "LOWER_THRESHOLD": -np.inf,
            "UPPER_THRESHOLD": 80.0,
            "RESULT": "FAIL",
            "SHOP_ID": "ALL_SHOPS",
        }

    def test_iqr_outlier_check_failure_oven_2024_02_13(self, iqr_outlier_check, dummy_table_iqr_oven):
        check = iqr_outlier_check(
            check_column="VALUE",
            table=self.table_builder._dataset + "." + dummy_table_iqr_oven.table_name,
            date="2024-02-13",
            date_filter_column="BQ_PARTITIONTIME",
            interval_days=14,
            how="upper",
            iqr_factor=1.5,
        )
        _ = self.runner.run(
            check.query,
            [dummy_table_iqr_oven],
        )
        result = check(bq_client=self.bq_client)
        assert result == {
            "COLUMN": "VALUE",
            "METRIC_NAME": "VALUE_outlier_iqr_upper_1_5",
            "TABLE": self.table_builder._dataset + "." + dummy_table_iqr_oven.table_name,
            "DATE": "2024-02-13",
            "VALUE": 583.0,
            "LOWER_THRESHOLD": -np.inf,
            "UPPER_THRESHOLD": 86.375,
            "RESULT": "FAIL",
            "SHOP_ID": "ALL_SHOPS",
        }
