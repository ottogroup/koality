"""Integration tests for CheckExecutor."""

from pathlib import Path
from textwrap import dedent

import duckdb
import pytest
from pydantic_yaml import parse_yaml_raw_as

from koality.executor import CheckExecutor
from koality.models import Config


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
