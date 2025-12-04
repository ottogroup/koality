import pytest

pytestmark = pytest.mark.integration


class TestMatchRateCheck:
    @classmethod
    def setup_class(cls):
        cls.table_builder = BQTableDefinitionBuilder("test-project-dev")
        cls.runner = SQLRunner(bq.Client(project="test-project-dev"))

    @pytest.fixture(scope="class")
    def purchase_table_raw(self):
        return pd.DataFrame(
            {
                "DATE": ["2023-01-01"] * 6 + ["2022-12-31", "2023-01-02"],
                "shop_code": ["SHOP006"] + ["SHOP001"] * 7,
                "product_number": [
                    "SHOP006-0001",  # irrelevant shopId
                    "SHOP001-0001",
                    "SHOP001-0002",
                    "SHOP001-0003",
                    "SHOP001-0001",  # second purchase of this product also relevant
                    "SHOP001-9999",  # cannot be found in skufeed
                    "SHOP001-0040",  # too early, should be ignored
                    "SHOP001-0040",  # too late, should be ignored
                ],
            }
        )

    @pytest.fixture(scope="class")
    def purchase_table(self, purchase_table_raw):
        return self.table_builder.from_df(name="dataset.purchase_order", df=purchase_table_raw)

    @pytest.fixture(scope="class")
    def purchase_table_renamed(self, purchase_table_raw):
        return self.table_builder.from_df(
            name="dataset.purchase_order",
            df=purchase_table_raw.rename(
                columns={"product_number": "product_number_v2"},
            ),
        )

    @pytest.fixture(scope="class")
    def skufeed_table(self):
        df = pd.DataFrame(
            {
                "DATE": ["2022-12-31", "2023-01-02"] + ["2023-01-01"] * 8,
                "shop_code": ["SHOP001"] * 9 + ["SHOP006"],
                "product_number": [
                    "SHOP001-9999",  # too early
                    "SHOP001-9999",  # too late
                    "SHOP001-0001",
                    "SHOP001-0001",  # only one entry relevant due to DISTINCT
                    "SHOP001-0002",
                    "SHOP001-0002",  # only one entry relevant due to DISTINCT
                    "SHOP001-0003",
                    "SHOP001-0003",  # only one entry relevant due to DISTINCT
                    "SHOP001-0040",  # not purchased
                    "SHOP006-0001",  # irrelevant shopId
                ],
            }
        )

        return self.table_builder.from_df(name="dataset.skufeed", df=df)

    @pytest.fixture()
    def match_rate_check(self):
        from koality.checks import MatchRateCheck

        return MatchRateCheck

    def test_match_rate_check(self, match_rate_check, skufeed_table, purchase_table):
        """
        Simple check for match rate:
        - 4 / 5 product_numbers of purchases should be found
        - tests if data before / after check day are excluded
        - tests if data of other shops are excluded
        """

        # build check
        check = match_rate_check(
            date="2023-01-01",
            left_table=self.table_builder._dataset + "." + purchase_table.table_name,
            right_table=self.table_builder._dataset + "." + skufeed_table.table_name,
            shop_id="SHOP001",
            join_columns=[
                "product_number",
            ],
            check_column="product_number",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [skufeed_table, purchase_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_matchrate": [
                    0.8,  # 4 / 5 product_numbers found
                ]
            }
        )

        assert_frame_equal(result_df, expected_df)

    def test_match_rate_check_join_via_2(self, match_rate_check, skufeed_table, purchase_table):
        """
        Tests if check also works if join is done via more than 1 column.
        """

        # build check
        check = match_rate_check(
            date="2023-01-01",
            left_table=self.table_builder._dataset + "." + purchase_table.table_name,
            right_table=self.table_builder._dataset + "." + skufeed_table.table_name,
            shop_id="SHOP001",
            join_columns=["DATE", "product_number"],
            check_column="product_number",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [skufeed_table, purchase_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_matchrate": [
                    0.8,  # 4 / 5 product_numbers found
                ]
            }
        )

        assert_frame_equal(result_df, expected_df)

    def test_match_rate_check_different_join_col_names(self, match_rate_check, skufeed_table, purchase_table_renamed):
        """
        Tests if check also works if join is done via columns with different names.
        """

        # build check
        check = match_rate_check(
            date="2023-01-01",
            left_table=self.table_builder._dataset + "." + purchase_table_renamed.table_name,
            right_table=self.table_builder._dataset + "." + skufeed_table.table_name,
            shop_id="SHOP001",
            join_columns_left=["DATE", "product_number_v2"],
            join_columns_right=["DATE", "product_number"],
            check_column="product_number",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [skufeed_table, purchase_table_renamed],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_matchrate": [
                    0.8,  # 4 / 5 product_numbers found
                ]
            }
        )

        assert_frame_equal(result_df, expected_df)

    @pytest.mark.parametrize(
        "day,shop",
        [
            ("2023-01-01", "SHOP999"),
            ("2022-12-30", "SHOP001"),
            ("2023-01-03", "SHOP001"),
            ("2023-01-03", "SHOP999"),
        ],
    )
    def test_match_rate_check_no_data(self, match_rate_check, skufeed_table, purchase_table, day, shop):
        """
        Test check if there is no data for the shop / day combination.
        """

        # build check
        check = match_rate_check(
            date=day,
            left_table=f"{self.table_builder._dataset}.{purchase_table.table_name}",
            right_table=f"{self.table_builder._dataset}.{skufeed_table.table_name}",
            shop_id=shop,
            join_columns=[
                "product_number",
            ],
            check_column="product_number",
            shop_id_filter_column="shop_code",
            date_filter_column="DATE",
        )

        # run query
        result_df = self.runner.run(
            check.query,
            [skufeed_table, purchase_table],
        )

        # compare with expected
        expected_df = pd.DataFrame(
            {
                "product_number_matchrate": [
                    np.nan,
                ]
            }
        )

        assert_frame_equal(result_df, expected_df)
