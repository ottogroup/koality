"""Unit tests for CheckExecutor."""

import math
from pathlib import Path
from textwrap import dedent

import pytest
from pydantic_yaml import parse_yaml_raw_as

from koality.checks import CountCheck, NullRatioCheck
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
def test_cache_key_generation() -> None:
    """Test that cache keys are generated correctly for check instances."""
    # Create two check instances with same parameters
    check1 = NullRatioCheck(
        database_accessor="project.dataset",
        database_provider=None,
        table="test_table",
        check_column="value",
        filters={
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
            "shop": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
        },
    )

    check2 = NullRatioCheck(
        database_accessor="project.dataset",
        database_provider=None,
        table="test_table",
        check_column="other_value",  # Different column, but same dataset
        filters={
            "date": {"column": "DATE", "value": "2023-01-01", "type": "date"},
            "shop": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
        },
    )

    # Create a check with different date
    check3 = NullRatioCheck(
        database_accessor="project.dataset",
        database_provider=None,
        table="test_table",
        check_column="value",
        filters={
            "date": {"column": "DATE", "value": "2023-01-02", "type": "date"},
            "shop": {"column": "shop_code", "value": "SHOP001", "type": "identifier"},
        },
    )

    # Generate cache keys
    key1 = CheckExecutor._get_dataset_cache_key(check1)  # noqa: SLF001
    key2 = CheckExecutor._get_dataset_cache_key(check2)  # noqa: SLF001
    key3 = CheckExecutor._get_dataset_cache_key(check3)  # noqa: SLF001

    # Checks 1 and 2 should have the same cache key (same dataset)
    assert key1 == key2

    # Check 3 should have a different cache key (different date)
    assert key1 != key3


@pytest.mark.unit
def test_cache_key_with_no_filters() -> None:
    """Test cache key generation when no filters are present."""
    check = CountCheck(
        database_accessor="project.dataset",
        database_provider=None,
        table="test_table",
        check_column="*",
        filters={},
    )

    key = CheckExecutor._get_dataset_cache_key(check)  # noqa: SLF001

    # Should return a valid tuple
    assert isinstance(key, tuple)
    assert key[0] == "test_table"
    assert key[1] == "project.dataset"
    assert key[2] is None  # No date filter
    assert key[3] == frozenset()  # Empty filters


@pytest.mark.unit
def test_yaml_parsing_infinite_thresholds(tmp_path: Path) -> None:
    """Test that YAML parsing correctly handles .inf and -.inf threshold values."""
    content = dedent(
        f"""
        name: test-infinite-thresholds

        database_setup: ""
        database_accessor: ""

        defaults:
          monitor_only: False
          log_path: {tmp_path}/message.txt

        check_bundles:
          - name: test-bundle
            defaults:
              check_type: CountCheck
              table: test_table
              check_column: "*"
            checks:
              - shop_id: TEST001
                lower_threshold: -.inf
                upper_threshold: .inf
              - shop_id: TEST002
                lower_threshold: 0
                upper_threshold: .inf
              - shop_id: TEST003
                lower_threshold: -.inf
                upper_threshold: 100
        """,
    ).strip()

    config = parse_yaml_raw_as(Config, content)

    # Verify that the config was parsed successfully
    assert config.name == "test-infinite-thresholds"
    assert len(config.check_bundles) == 1
    bundle = config.check_bundles[0]
    assert len(bundle.checks) == 3

    # Verify first check has both infinite thresholds
    check1 = bundle.checks[0]
    assert check1.lower_threshold == -math.inf
    assert check1.upper_threshold == math.inf

    # Verify second check has infinite upper threshold
    check2 = bundle.checks[1]
    assert check2.lower_threshold == 0
    assert check2.upper_threshold == math.inf

    # Verify third check has infinite lower threshold
    check3 = bundle.checks[2]
    assert check3.lower_threshold == -math.inf
    assert check3.upper_threshold == 100
