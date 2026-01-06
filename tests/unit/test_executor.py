"""Unit tests for CheckExecutor."""

from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

import pytest
from pydantic_yaml import parse_yaml_raw_as

from koality.executor import CheckExecutor
from koality.models import Config


@pytest.fixture
def minimal_config(tmp_path: Path) -> Config:
    """Create a minimal config for testing."""
    content = dedent(
        f"""
        name: minimal-config

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: bundle-1
            defaults:
              check_type: CountCheck
              table: test_table
              check_column: "*"
              lower_threshold: 0
              upper_threshold: 1000
            checks:
              - shop_id: SHOP001
              - shop_id: SHOP002
              - shop_id: SHOP003
        """,
    ).strip()
    return parse_yaml_raw_as(Config, content)


@pytest.mark.unit
def test_progress_bar_initialization(minimal_config: Config) -> None:
    """Test that progress bar is initialized with correct total count."""
    mock_conn = MagicMock()
    executor = CheckExecutor(config=minimal_config, duckdb_client=mock_conn)

    with patch("koality.executor.tqdm") as mock_tqdm:
        mock_pbar = mock_tqdm.return_value.__enter__.return_value
        mock_pbar.update = MagicMock()

        # Mock check execution to avoid actual DB queries
        with patch("koality.executor.CHECK_MAP") as mock_check_map:
            mock_check_instance = MagicMock()
            mock_check_instance.return_value = {"RESULT": "PASS"}
            mock_check_instance.status = "PASS"
            mock_check_factory = MagicMock(return_value=mock_check_instance)
            mock_check_map.__getitem__ = MagicMock(return_value=mock_check_factory)

            executor.execute_checks()

            # Verify tqdm was initialized with total count of checks
            mock_tqdm.assert_called_once()
            call_kwargs = mock_tqdm.call_args.kwargs
            assert call_kwargs["total"] == 3  # 3 checks in the config
            assert call_kwargs["desc"] == "Executing checks"
            assert call_kwargs["unit"] == "check"


@pytest.mark.unit
def test_progress_bar_updates(minimal_config: Config) -> None:
    """Test that progress bar is updated after each check execution."""
    mock_conn = MagicMock()
    executor = CheckExecutor(config=minimal_config, duckdb_client=mock_conn)

    with patch("koality.executor.tqdm") as mock_tqdm:
        mock_pbar = mock_tqdm.return_value.__enter__.return_value
        mock_pbar.update = MagicMock()

        # Mock check execution
        with patch("koality.executor.CHECK_MAP") as mock_check_map:
            mock_check_instance = MagicMock()
            mock_check_instance.return_value = {"RESULT": "PASS"}
            mock_check_instance.status = "PASS"
            mock_check_factory = MagicMock(return_value=mock_check_instance)
            mock_check_map.__getitem__ = MagicMock(return_value=mock_check_factory)

            executor.execute_checks()

            # Verify update was called 3 times (once per check)
            assert mock_pbar.update.call_count == 3
            # Each update should be called with 1
            for call in mock_pbar.update.call_args_list:
                assert call[0][0] == 1


@pytest.mark.unit
def test_progress_bar_multiple_bundles() -> None:
    """Test progress bar with multiple check bundles."""
    content = dedent(
        """
        name: multi-bundle-config

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False

        check_bundles:
          - name: bundle-1
            defaults:
              check_type: CountCheck
              table: test_table
              check_column: "*"
              lower_threshold: 0
              upper_threshold: 1000
            checks:
              - shop_id: SHOP001
              - shop_id: SHOP002

          - name: bundle-2
            defaults:
              check_type: NullRatioCheck
              table: test_table
              check_column: value
              lower_threshold: 0
              upper_threshold: 0.1
            checks:
              - shop_id: SHOP003
              - shop_id: SHOP004
              - shop_id: SHOP005
        """,
    ).strip()
    config = parse_yaml_raw_as(Config, content)

    mock_conn = MagicMock()
    executor = CheckExecutor(config=config, duckdb_client=mock_conn)

    with patch("koality.executor.tqdm") as mock_tqdm:
        mock_pbar = mock_tqdm.return_value.__enter__.return_value
        mock_pbar.update = MagicMock()

        # Mock check execution
        with patch("koality.executor.CHECK_MAP") as mock_check_map:
            mock_check_instance = MagicMock()
            mock_check_instance.return_value = {"RESULT": "PASS"}
            mock_check_instance.status = "PASS"
            mock_check_factory = MagicMock(return_value=mock_check_instance)
            mock_check_map.__getitem__ = MagicMock(return_value=mock_check_factory)

            executor.execute_checks()

            # Verify total is sum of all checks across bundles (2 + 3 = 5)
            call_kwargs = mock_tqdm.call_args.kwargs
            assert call_kwargs["total"] == 5

            # Verify update was called 5 times
            assert mock_pbar.update.call_count == 5
