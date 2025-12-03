import numpy as np
import pandas as pd
import pytest
from bquest.dataframe import assert_frame_equal
from bquest.runner import SQLRunner
from bquest.tables import BQTableDefinitionBuilder
from google.cloud import bigquery as bq


@pytest.mark.bquest
class TestRelCountChangedCheck:
    @classmethod
    def setup_class(cls):
        cls.table_builder = BQTableDefinitionBuilder("test-project-dev")
        cls.runner = SQLRunner(bq.Client(project="test-project-dev"))

    @pytest.fixture(scope="class")
    def dummy_table(self):
        df = pd.DataFrame(
            {
                "DATE": [pd.Timestamp("2022-12-30", tz="UTC")] * 8  # other shop
                + [pd.Timestamp("2022-12-31", tz="UTC")] * 4
                + [pd.Timestamp("2023-01-01", tz="UTC")] * 4
                + [pd.Timestamp("2023-01-02", tz="UTC")] * 8
                + [pd.Timestamp("2023-01-03", tz="UTC")] * 6,
                "shop_id": ["SHOP006"] * 8 + ["SHOP001"] * 22,
                "product_number": [f"SHOP006-{idx + 1:04d}" for idx in range(8)]  # other shop
                + [f"SHOP001-{idx + 1:04d}" for idx in range(4)]
                + [f"SHOP001-{idx + 1:04d}" for idx in range(4)]
                + [f"SHOP001-{idx + 1:04d}" for idx in range(8)]
                + [f"SHOP001-{idx + 1:04d}" for idx in range(6)],
            }
        )

        return self.table_builder.from_df(name="dataset.dummy_table", df=df)

    @pytest.fixture()
    def rel_count_change_check(self):
        from koality.checks import RelCountChangeCheck

        return RelCountChangeCheck

    @pytest.mark.parametrize(
        "day,change_rate",
        [
            ("2023-01-02", 1.0),  # (8 - 4) / 4
            ("2023-01-03", 0.0),  # (6 - 6) / 6
            ("2022-12-31", np.nan),  # no history
            ("2023-01-04", -1.0),  # (0 - 7) / 7, no current data
        ],
    )
    def test_rel_count_change_check_shop_filter(self, rel_count_change_check, dummy_table, day, change_rate):
        """
        Test cases with shop restriction and different change rates
        for different days, including no data for history and no
        data for check day.
        """

        # build check
        check = rel_count_change_check(
            date=day,
            table=f"{self.table_builder._dataset}.{dummy_table.table_name}",
            shop_id="SHOP001",
            check_column="product_number",
            shop_id_filter_column="shop_id",
            date_filter_column="DATE",
            rolling_days=2,
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_count_change": [
                    change_rate,
                ]
            }
        )

        assert_frame_equal(result_df, expected_df)

    def test_rel_count_change_check_no_shop_filter(self, rel_count_change_check, dummy_table):
        """
        Test cases without shop restriction, and this now with history leading
        to a decreasing number of rows.
        """

        # build check
        check = rel_count_change_check(
            date="2022-12-31",
            table=f"{self.table_builder._dataset}.{dummy_table.table_name}",
            check_column="product_number",
            date_filter_column="DATE",
            rolling_days=2,
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [dummy_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_count_change": [
                    -0.5,  # (4 - 8) / 8
                ]
            }
        )

        assert_frame_equal(result_df, expected_df)
