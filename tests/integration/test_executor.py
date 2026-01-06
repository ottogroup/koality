"""Integration tests for CheckExecutor."""

from pathlib import Path
from textwrap import dedent
from unittest.mock import patch

import duckdb
import pytest
from pydantic_yaml import parse_yaml_raw_as

from koality.executor import CheckExecutor
from koality.models import Config

pytestmark = pytest.mark.integration


@pytest.fixture
def duckdb_client() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with test data."""
    conn = duckdb.connect(":memory:")
    # Create a test table with sample data
    conn.execute("""
        CREATE TABLE dummy_table (
            shop_code VARCHAR,
            DATE DATE,
            value FLOAT,
            product_number VARCHAR
        )
    """)
    # Insert test data for different shops with product numbers matching pattern SHOP001-XXXX
    conn.execute("""
        INSERT INTO dummy_table
        SELECT 'SHOP001', '2023-01-01'::DATE, random(), 'SHOP001-' || LPAD(CAST(i AS VARCHAR), 4, '0')
        FROM range(100) AS t(i)
    """)
    conn.execute("""
        INSERT INTO dummy_table
        SELECT 'SHOP002', '2023-01-01'::DATE, random(), 'SHOP002-' || LPAD(CAST(i AS VARCHAR), 4, '0')
        FROM range(100) AS t(i)
    """)
    return conn


@pytest.fixture
def config_file_success(tmp_path: Path) -> Path:
    """Create a config file with all checks expected to succeed."""
    content = dedent(
        f"""
        name: koality-all-success

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: check-bundle-1
            defaults:
              check_type: CountCheck
              table: dummy_table
              check_column: "*"
              lower_threshold: 0
              upper_threshold: 1000
            checks:
              - shop_id: SHOP001
              - shop_id: SHOP002

          - name: check-bundle-2
            defaults:
              check_type: CountCheck
              table: dummy_table
              check_column: "*"
              lower_threshold: 0
              upper_threshold: 1000
            checks:
              - shop_id: SHOP001
              - shop_id: SHOP002
        """,
    ).strip()
    tmp_file = tmp_path / "koality_config.yaml"
    tmp_file.write_text(content)
    return tmp_file


@pytest.fixture
def config_file_failure(tmp_path: Path) -> Path:
    """Create a config file with a check expected to fail."""
    content = dedent(
        f"""
        name: koality-failure

        defaults:
          monitor_only: False
          result_table: dataquality.data_koality_monitoring
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: check-bundle-1
            defaults:
              check_type: SuccessCheck
            checks:
              - shop_id: SHOP001
                name_suffix: "1-1"
              - shop_id: SHOP002
                check_type: FailureCheck
                name_suffix: "1-2"
        """,
    ).strip()
    tmp_file = tmp_path / "koality_config.yaml"
    tmp_file.write_text(content)
    return tmp_file


@pytest.fixture
def config_file_failure_v2(tmp_path: Path) -> Path:
    """Config with one successful and one failing check using real check types."""
    content = dedent(
        f"""
        name: koality-failure

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: check-bundle-1
            defaults:
              check_type: CountCheck
              table: dummy_table
              check_column: "*"
            checks:
              - shop_id: SHOP001
                lower_threshold: 0
                upper_threshold: 1000
              - shop_id: SHOP002
                lower_threshold: 0
                upper_threshold: 10
        """,
    ).strip()
    tmp_file = tmp_path / "koality_config.yaml"
    tmp_file.write_text(content)
    return tmp_file


@pytest.fixture
def config_file_missing_data_v2(tmp_path: Path) -> Path:
    """Config that queries a non-existent table to trigger data_exists failure."""
    content = dedent(
        f"""
        name: koality-missing-data

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: check-bundle-1
            defaults:
              check_type: CountCheck
              table: empty_table
              check_column: "*"
              lower_threshold: 0
              upper_threshold: 1000
            checks:
              - shop_id: SHOP001
              - shop_id: SHOP002
        """,
    ).strip()
    tmp_file = tmp_path / "koality_config.yaml"
    tmp_file.write_text(content)
    return tmp_file


def test_executor_all_success(config_file_success: Path, duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test executor passes when all checks succeed."""
    config = parse_yaml_raw_as(Config, config_file_success.read_text())
    executor = CheckExecutor(config=config, duckdb_client=duckdb_client)
    result_dict = executor()
    result = {item["METRIC_NAME"] for item in result_dict}
    # CountCheck with check_column="*" produces "row_count" metric name
    expected = {"row_count"}

    assert result == expected
    assert not Path(config_file_success.parent, "message.txt").exists()
    assert not executor.check_failed


def test_executor_failure(config_file_failure_v2: Path, duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test executor reports failure when a check fails threshold."""
    config = parse_yaml_raw_as(Config, config_file_failure_v2.read_text())
    executor = CheckExecutor(config=config, duckdb_client=duckdb_client)
    result_dict = executor()
    result = {item["METRIC_NAME"] for item in result_dict}
    expected = {"row_count"}
    assert result == expected
    message_file = Path(config_file_failure_v2.parent, "message.txt")
    assert message_file.exists()
    check_message = message_file.read_text()
    # SHOP002 has 100 rows but threshold is 0-10, so it should fail
    assert "row_count failed" in check_message
    assert "ALL" in check_message
    assert executor.check_failed is True


def test_executor_missing_data(config_file_missing_data_v2: Path, duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test executor reports failure when table has no data."""
    # Create empty table
    duckdb_client.execute("""
        CREATE TABLE empty_table (
            shop_code VARCHAR,
            DATE DATE,
            value FLOAT
        )
    """)

    config = parse_yaml_raw_as(Config, config_file_missing_data_v2.read_text())
    executor = CheckExecutor(config=config, duckdb_client=duckdb_client)
    _ = executor()
    message_file = Path(config_file_missing_data_v2.parent, "message.txt")
    assert message_file.exists()
    check_message = message_file.read_text()
    # Empty table should trigger "No data" message
    assert "No data" in check_message or "empty_table" in check_message
    assert executor.check_failed is True


def test_executor_regex_match_check(tmp_path: Path, duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test executor with RegexMatchCheck validates product_number pattern."""
    content = dedent(
        f"""
        name: koality-regex-check

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: regex-bundle
            defaults:
              check_type: RegexMatchCheck
              table: dummy_table
              check_column: product_number
              lower_threshold: 0.99
              upper_threshold: 1.0
              regex_to_match: 'SHOP\\d{{3}}-\\d{{4}}'
              filters:
                date:
                  column: DATE
                  value: "2023-01-01"
                  type: date
            checks:
              - filters:
                  shop_id:
                    column: shop_code
                    value: SHOP001
                    type: identifier
              - filters:
                  shop_id:
                    column: shop_code
                    value: SHOP002
                    type: identifier
        """,
    ).strip()
    tmp_file = tmp_path / "koality_config.yaml"
    tmp_file.write_text(content)

    config = parse_yaml_raw_as(Config, tmp_file.read_text())
    executor = CheckExecutor(config=config, duckdb_client=duckdb_client)
    result_dict = executor()

    # All product numbers match pattern SHOPXXX-XXXX
    result = {item["METRIC_NAME"] for item in result_dict}
    assert "product_number_regex_match_ratio" in result
    assert not executor.check_failed


def test_executor_progress_bar(config_file_success: Path, duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test that progress bar is displayed during check execution."""
    config = parse_yaml_raw_as(Config, config_file_success.read_text())
    executor = CheckExecutor(config=config, duckdb_client=duckdb_client)

    with patch("koality.executor.tqdm") as mock_tqdm:
        # Setup mock to return a context manager
        mock_pbar = mock_tqdm.return_value.__enter__.return_value

        executor()

        # Verify tqdm was called with correct parameters
        mock_tqdm.assert_called_once()
        call_kwargs = mock_tqdm.call_args.kwargs
        assert call_kwargs["total"] == 4  # config_file_success has 4 checks (2 bundles x 2 checks each)
        assert call_kwargs["desc"] == "Executing checks"
        assert call_kwargs["unit"] == "check"

        # Verify progress bar was updated 4 times (once per check)
        assert mock_pbar.update.call_count == 4
        # Each update should increment by 1
        for call in mock_pbar.update.call_args_list:
            assert call[0][0] == 1


def test_data_existence_cache(tmp_path: Path, duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test that data existence checks are cached for checks on the same dataset."""
    content = dedent(
        f"""
        name: koality-cache-test

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: check-bundle-1
            defaults:
              table: dummy_table
              filters:
                date:
                  column: DATE
                  value: "2023-01-01"
                  type: date
                shop:
                  column: shop_code
                  value: SHOP001
                  type: identifier
            checks:
              - check_type: NullRatioCheck
                check_column: value
                lower_threshold: 0
                upper_threshold: 1
              - check_type: CountCheck
                check_column: "*"
                lower_threshold: 0
                upper_threshold: 1000
              - check_type: RegexMatchCheck
                check_column: product_number
                regex_to_match: 'SHOP\\d{{3}}-.*'
                lower_threshold: 0.99
                upper_threshold: 1.0
        """,
    ).strip()

    config = parse_yaml_raw_as(Config, content)
    executor = CheckExecutor(config=config, duckdb_client=duckdb_client)

    # Track data_check calls by patching execute_query
    from unittest.mock import patch

    from koality.utils import execute_query

    data_check_query_calls = []

    def track_query(query, client, provider):
        """Track queries that check for data existence."""
        if "IF(COUNT(*) > 0" in query or "IF(COUNTIF" in query:
            data_check_query_calls.append(query)
        return execute_query(query, client, provider)

    with patch("koality.checks.execute_query", side_effect=track_query):
        result_dict = executor()

    # All 3 checks are on the same dataset (table, date, filters)
    # So data_check query should only be executed once (cached for subsequent checks)
    assert len(data_check_query_calls) == 1

    # Verify all checks executed successfully
    assert len(result_dict) == 3
    assert not executor.check_failed


def test_data_existence_cache_different_datasets(tmp_path: Path, duckdb_client: duckdb.DuckDBPyConnection) -> None:
    """Test that cache is properly partitioned for different datasets."""
    content = dedent(
        f"""
        name: koality-cache-different-datasets

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: check-bundle-1
            defaults:
              check_type: CountCheck
              table: dummy_table
              check_column: "*"
              lower_threshold: 0
              upper_threshold: 1000
            checks:
              - filters:
                  date:
                    column: DATE
                    value: "2023-01-01"
                    type: date
                  shop:
                    column: shop_code
                    value: SHOP001
                    type: identifier
              - filters:
                  date:
                    column: DATE
                    value: "2023-01-01"
                    type: date
                  shop:
                    column: shop_code
                    value: SHOP002
                    type: identifier
        """,
    ).strip()

    config = parse_yaml_raw_as(Config, content)
    executor = CheckExecutor(config=config, duckdb_client=duckdb_client)

    # Track data_check calls by patching execute_query
    from unittest.mock import patch

    from koality.utils import execute_query

    data_check_query_calls = []

    def track_query(query, client, provider):
        """Track queries that check for data existence."""
        if "IF(COUNT(*) > 0" in query or "IF(COUNTIF" in query:
            data_check_query_calls.append(query)
        return execute_query(query, client, provider)

    with patch("koality.checks.execute_query", side_effect=track_query):
        result_dict = executor()

    # Two checks with different shop filters = different datasets
    # So data_check query should be executed twice (once per unique dataset)
    assert len(data_check_query_calls) == 2

    # Verify all checks executed successfully
    assert len(result_dict) == 2
    assert not executor.check_failed
