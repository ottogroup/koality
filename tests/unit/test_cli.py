"""Unit tests for CLI commands."""

import json
from pathlib import Path
from textwrap import dedent

import click
import pytest
import yaml
from click.testing import CliRunner

from koality.cli import (
    DATABASE_SETUP_VARIABLES_ENV,
    _apply_overwrites_to_dict,
    _get_variables_with_env,
    _parse_env_variables,
    _parse_overwrites,
    _parse_variables,
    cli,
)

pytestmark = pytest.mark.unit

VALID_CONFIG = dedent("""\
    name: test_config
    database_setup: ""
    database_accessor: memory

    defaults:
      monitor_only: true

    check_bundles:
      - name: test_bundle
        checks:
          - check_type: CountCheck
            table: test_table
            check_column: id
            lower_threshold: 0
            upper_threshold: 100
    """)

INVALID_CONFIG = dedent("""\
    name: missing_required_fields
    """)


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def valid_config_file(tmp_path: Path) -> Path:
    """Create a valid configuration file."""
    config_path = tmp_path / "valid_config.yaml"
    config_path.write_text(VALID_CONFIG)
    return config_path


@pytest.fixture
def invalid_config_file(tmp_path: Path) -> Path:
    """Create an invalid configuration file."""
    config_path = tmp_path / "invalid_config.yaml"
    config_path.write_text(INVALID_CONFIG)
    return config_path


class TestCliGroup:
    """Tests for the main CLI group."""

    def test_cli_help(self, runner: CliRunner) -> None:
        """Test that CLI shows help message."""
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Koality - Data quality monitoring CLI" in result.output
        assert "run" in result.output
        assert "validate" in result.output
        assert "print" in result.output

    def test_cli_no_command(self, runner: CliRunner) -> None:
        """Test that CLI without command shows usage error."""
        result = runner.invoke(cli, [])
        assert result.exit_code == 2
        assert "Usage:" in result.output


class TestValidateCommand:
    """Tests for the validate command."""

    def test_validate_valid_config(self, runner: CliRunner, valid_config_file: Path) -> None:
        """Test validate command with valid configuration."""
        result = runner.invoke(cli, ["validate", "--config_path", str(valid_config_file)])
        assert result.exit_code == 0
        assert "is valid" in result.output

    def test_validate_invalid_config(self, runner: CliRunner, invalid_config_file: Path) -> None:
        """Test validate command with invalid configuration."""
        result = runner.invoke(cli, ["validate", "--config_path", str(invalid_config_file)])
        assert result.exit_code == 1
        assert "is invalid" in result.output

    def test_validate_missing_file(self, runner: CliRunner) -> None:
        """Test validate command with non-existent file."""
        result = runner.invoke(cli, ["validate", "--config_path", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_validate_missing_config_path(self, runner: CliRunner) -> None:
        """Test validate command without config_path option."""
        result = runner.invoke(cli, ["validate"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()


class TestPrintCommand:
    """Tests for the print command."""

    def test_print_yaml_format(self, runner: CliRunner, valid_config_file: Path) -> None:
        """Test print command with YAML format (default)."""
        result = runner.invoke(cli, ["print", "--config_path", str(valid_config_file)])
        assert result.exit_code == 0
        parsed = yaml.safe_load(result.output)
        assert parsed["name"] == "test_config"

    def test_print_json_format(self, runner: CliRunner, valid_config_file: Path) -> None:
        """Test print command with JSON format."""
        result = runner.invoke(cli, ["print", "--config_path", str(valid_config_file), "--format", "json"])
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert parsed["name"] == "test_config"

    def test_print_model_format(self, runner: CliRunner, valid_config_file: Path) -> None:
        """Test print command with model format."""
        result = runner.invoke(cli, ["print", "--config_path", str(valid_config_file), "--format", "model"])
        assert result.exit_code == 0
        assert "test_config" in result.output

    def test_print_custom_indent(self, runner: CliRunner, valid_config_file: Path) -> None:
        """Test print command with custom indentation."""
        result = runner.invoke(
            cli,
            ["print", "--config_path", str(valid_config_file), "--format", "json", "--indent", "4"],
        )
        assert result.exit_code == 0
        # Verify 4-space indentation in output
        assert '    "name"' in result.output

    def test_print_invalid_config(self, runner: CliRunner, invalid_config_file: Path) -> None:
        """Test print command with invalid configuration."""
        result = runner.invoke(cli, ["print", "--config_path", str(invalid_config_file)])
        assert result.exit_code == 1
        assert "is invalid" in result.output

    def test_print_missing_file(self, runner: CliRunner) -> None:
        """Test print command with non-existent file."""
        result = runner.invoke(cli, ["print", "--config_path", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_print_with_overwrites(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test print command with overwrites option."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: memory

            defaults:
              monitor_only: true
              filters:
                partition_date:
                  column: DATE
                  value: yesterday
                  type: date

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
                "--format",
                "json",
                "-o",
                "partition_date=2023-06-15",
            ],
        )
        assert result.exit_code == 0
        # Verify the overwritten value appears in output
        assert "2023-06-15" in result.output
        # Parse JSON and verify the check-level filter was overwritten
        parsed = json.loads(result.output)
        check_filter_value = parsed["check_bundles"][0]["checks"][0]["filters"]["partition_date"]["value"]
        assert check_filter_value == "2023-06-15"


class TestRunCommand:
    """Tests for the run command."""

    def test_run_help(self, runner: CliRunner) -> None:
        """Test run command shows help."""
        result = runner.invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "Run koality checks" in result.output
        assert "--config_path" in result.output

    def test_run_missing_config_path(self, runner: CliRunner) -> None:
        """Test run command without config_path option."""
        result = runner.invoke(cli, ["run"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_run_missing_file(self, runner: CliRunner) -> None:
        """Test run command with non-existent file."""
        result = runner.invoke(cli, ["run", "--config_path", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_run_help_shows_overwrites_option(self, runner: CliRunner) -> None:
        """Test that run command help shows overwrites option."""
        result = runner.invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--overwrites" in result.output or "-o" in result.output


class TestOverwrites:
    """Tests for the overwrites functionality."""

    def test_parse_overwrites_valid(self) -> None:
        """Test parsing valid overwrite arguments."""
        overwrites = ("filters.partition_date=2023-01-01", "filters.shop_id=SHOP02")
        result = _parse_overwrites(overwrites)
        assert result == [("filters.partition_date", "2023-01-01"), ("filters.shop_id", "SHOP02")]

    def test_parse_overwrites_with_spaces(self) -> None:
        """Test parsing overwrites with spaces around key/value."""
        overwrites = ("filters.partition_date = 2023-01-01",)
        result = _parse_overwrites(overwrites)
        assert result == [("filters.partition_date", "2023-01-01")]

    def test_parse_overwrites_value_with_equals(self) -> None:
        """Test parsing overwrites where value contains equals sign."""
        overwrites = ("filter=a=b=c",)
        result = _parse_overwrites(overwrites)
        assert result == [("filter", "a=b=c")]

    def test_parse_overwrites_invalid_format(self) -> None:
        """Test parsing invalid overwrite format raises error."""
        overwrites = ("invalid_no_equals",)
        with pytest.raises(click.BadParameter, match="Invalid overwrite format"):
            _parse_overwrites(overwrites)

    def test_parse_overwrites_empty(self) -> None:
        """Test parsing empty overwrites."""
        result = _parse_overwrites(())
        assert result == []

    def test_apply_overwrites_filter_value(self) -> None:
        """Test applying filter value overwrite to config dict."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "monitor_only": True,
                "filters": {
                    "partition_date": {
                        "column": "DATE",
                        "value": "yesterday",
                        "type": "date",
                    },
                },
            },
            "check_bundles": [
                {
                    "name": "test_bundle",
                    "checks": [
                        {
                            "check_type": "CountCheck",
                            "table": "test_table",
                            "check_column": "id",
                            "lower_threshold": 0,
                            "upper_threshold": 100,
                        },
                    ],
                },
            ],
        }

        # Verify initial value
        assert config_dict["defaults"]["filters"]["partition_date"]["value"] == "yesterday"

        # Apply overwrite using new path syntax
        _apply_overwrites_to_dict(config_dict, [("filters.partition_date", "2023-06-15")])

        # Verify overwritten value in defaults
        assert config_dict["defaults"]["filters"]["partition_date"]["value"] == "2023-06-15"

    def test_apply_overwrites_identifier_format(self) -> None:
        """Test applying identifier_format overwrite to config dict."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "identifier_format": "filter_name",
            },
            "check_bundles": [],
        }

        # Apply overwrite for identifier_format
        _apply_overwrites_to_dict(config_dict, [("identifier_format", "column_name")])

        # Verify overwritten value
        assert config_dict["defaults"]["identifier_format"] == "column_name"

    def test_apply_overwrites_monitor_only(self) -> None:
        """Test applying monitor_only boolean overwrite."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "monitor_only": True,
            },
            "check_bundles": [],
        }

        # Apply overwrite for monitor_only
        _apply_overwrites_to_dict(config_dict, [("monitor_only", "false")])

        # Verify overwritten value (should be converted to bool)
        assert config_dict["defaults"]["monitor_only"] is False

    def test_apply_overwrites_bundle_level(self) -> None:
        """Test applying overwrite at bundle level."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "identifier_format": "filter_name",
            },
            "check_bundles": [
                {
                    "name": "my_bundle",
                    "checks": [],
                },
            ],
        }

        # Apply overwrite at bundle level using explicit check_bundles prefix
        _apply_overwrites_to_dict(config_dict, [("check_bundles.my_bundle.identifier_format", "column_name")])

        # Verify bundle-level override
        assert config_dict["check_bundles"][0]["defaults"]["identifier_format"] == "column_name"
        # Global default should be unchanged
        assert config_dict["defaults"]["identifier_format"] == "filter_name"

    def test_apply_overwrites_bundle_filter(self) -> None:
        """Test applying filter overwrite at bundle level."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "filters": {
                    "partition_date": {
                        "column": "DATE",
                        "value": "yesterday",
                        "type": "date",
                    },
                },
            },
            "check_bundles": [
                {
                    "name": "my_bundle",
                    "defaults": {
                        "filters": {
                            "partition_date": {
                                "column": "DATE",
                                "value": "yesterday",
                                "type": "date",
                            },
                        },
                    },
                    "checks": [],
                },
            ],
        }

        # Apply overwrite at bundle level only using explicit prefix
        _apply_overwrites_to_dict(config_dict, [("check_bundles.my_bundle.filters.partition_date", "2023-06-15")])

        # Verify bundle-level override
        assert config_dict["check_bundles"][0]["defaults"]["filters"]["partition_date"]["value"] == "2023-06-15"
        # Global default should be unchanged
        assert config_dict["defaults"]["filters"]["partition_date"]["value"] == "yesterday"

    def test_apply_overwrites_check_level(self) -> None:
        """Test applying overwrite at specific check level."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {},
            "check_bundles": [
                {
                    "name": "my_bundle",
                    "checks": [
                        {
                            "check_type": "CountCheck",
                            "table": "test_table",
                            "check_column": "id",
                            "lower_threshold": 0,
                            "upper_threshold": 100,
                        },
                        {
                            "check_type": "NullRatioCheck",
                            "table": "test_table",
                            "check_column": "name",
                            "upper_threshold": 0.1,
                        },
                    ],
                },
            ],
        }

        # Apply overwrite to specific check by index using explicit prefix
        _apply_overwrites_to_dict(config_dict, [("check_bundles.my_bundle.0.table", "other_table")])

        # Verify check-level override
        assert config_dict["check_bundles"][0]["checks"][0]["table"] == "other_table"
        # Second check should be unchanged
        assert config_dict["check_bundles"][0]["checks"][1]["table"] == "test_table"

    def test_apply_overwrites_check_filter(self) -> None:
        """Test applying filter overwrite at specific check level."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {},
            "check_bundles": [
                {
                    "name": "my_bundle",
                    "checks": [
                        {
                            "check_type": "CountCheck",
                            "table": "test_table",
                            "check_column": "id",
                            "filters": {
                                "partition_date": {
                                    "column": "DATE",
                                    "value": "yesterday",
                                    "type": "date",
                                },
                            },
                        },
                    ],
                },
            ],
        }

        # Apply filter overwrite to specific check using explicit prefix
        _apply_overwrites_to_dict(config_dict, [("check_bundles.my_bundle.0.filters.partition_date", "2023-06-15")])

        # Verify check-level filter override
        assert config_dict["check_bundles"][0]["checks"][0]["filters"]["partition_date"]["value"] == "2023-06-15"

    def test_apply_overwrites_creates_nonexistent_filter(self) -> None:
        """Test applying overwrite for non-existent filter creates it."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "monitor_only": True,
                "filters": {
                    "partition_date": {
                        "column": "DATE",
                        "value": "yesterday",
                        "type": "date",
                    },
                },
            },
            "check_bundles": [],
        }

        # Apply overwrite for non-existent filter - this creates the filter
        _apply_overwrites_to_dict(config_dict, [("filters.new_filter", "some_value")])

        # Original filter should be unchanged
        assert config_dict["defaults"]["filters"]["partition_date"]["value"] == "yesterday"
        # New filter should be created with the value
        assert config_dict["defaults"]["filters"]["new_filter"] == {"value": "some_value"}

    def test_apply_overwrites_check_level_inherited_filter(self) -> None:
        """Test applying overwrite at check level for filter inherited from defaults."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "filters": {
                    "date": {
                        "column": "DATE",
                        "value": "yesterday",
                        "type": "date",
                    },
                },
            },
            "check_bundles": [
                {
                    "name": "my_bundle",
                    "checks": [
                        {
                            "check_type": "CountCheck",
                            "table": "test_table",
                            "check_column": "id",
                            # Note: no filters defined - inherited from defaults
                        },
                    ],
                },
            ],
        }

        # Apply overwrite at check level for filter that's inherited from defaults
        _apply_overwrites_to_dict(config_dict, [("check_bundles.my_bundle.0.date", "today-2")])

        # Check should now have the filter with overwritten value
        check = config_dict["check_bundles"][0]["checks"][0]
        assert check["filters"]["date"] == {"value": "today-2"}

    def test_apply_overwrites_filter_column(self) -> None:
        """Test applying filter column overwrite."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "filters": {
                    "partition_date": {
                        "column": "DATE",
                        "value": "yesterday",
                        "type": "date",
                    },
                },
            },
            "check_bundles": [],
        }

        # Apply overwrite for filter column
        _apply_overwrites_to_dict(config_dict, [("filters.partition_date.column", "OTHER_DATE_COL")])

        # Verify column was overwritten
        assert config_dict["defaults"]["filters"]["partition_date"]["column"] == "OTHER_DATE_COL"
        # Value should be unchanged
        assert config_dict["defaults"]["filters"]["partition_date"]["value"] == "yesterday"

    def test_apply_overwrites_filter_operator(self) -> None:
        """Test applying filter operator overwrite."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "filters": {
                    "amount": {
                        "column": "total_amount",
                        "value": "100",
                        "operator": "=",
                    },
                },
            },
            "check_bundles": [],
        }

        # Apply overwrite for filter operator
        _apply_overwrites_to_dict(config_dict, [("filters.amount.operator", ">=")])

        # Verify operator was overwritten
        assert config_dict["defaults"]["filters"]["amount"]["operator"] == ">="

    def test_apply_overwrites_filter_explicit_value(self) -> None:
        """Test applying filter value with explicit .value path."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {
                "filters": {
                    "partition_date": {
                        "column": "DATE",
                        "value": "yesterday",
                        "type": "date",
                    },
                },
            },
            "check_bundles": [],
        }

        # Apply overwrite using explicit .value path
        _apply_overwrites_to_dict(config_dict, [("filters.partition_date.value", "2023-06-15")])

        # Verify value was overwritten
        assert config_dict["defaults"]["filters"]["partition_date"]["value"] == "2023-06-15"

    def test_overwrites_propagate_to_checks(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test that overwrites in defaults propagate to all checks."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: memory

            defaults:
              monitor_only: true
              filters:
                partition_date:
                  column: DATE
                  value: yesterday
                  type: date

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
                  - check_type: NullRatioCheck
                    table: test_table
                    check_column: name
                    upper_threshold: 0.1
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
                "--format",
                "json",
                "-o",
                "partition_date=2023-06-15",
            ],
        )
        assert result.exit_code == 0

        parsed = json.loads(result.output)

        # Verify defaults was overwritten
        assert parsed["defaults"]["filters"]["partition_date"]["value"] == "2023-06-15"

        # Verify both checks have the overwritten value (propagated from defaults)
        checks = parsed["check_bundles"][0]["checks"]
        assert checks[0]["filters"]["partition_date"]["value"] == "2023-06-15"
        assert checks[1]["filters"]["partition_date"]["value"] == "2023-06-15"

    def test_run_with_overwrites_cli(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test run command with overwrites option via CLI."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: ""

            defaults:
              monitor_only: true
              filters:
                partition_date:
                  column: DATE
                  value: yesterday
                  type: date

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        # Run with overwrites - this will fail due to missing table but
        # we can verify the option is accepted
        result = runner.invoke(
            cli,
            [
                "run",
                "--config_path",
                str(config_path),
                "-o",
                "partition_date=2023-06-15",
            ],
        )
        # The command accepts the option (exit code may be non-zero due to missing table)
        # but we verify it doesn't fail on parsing the option
        assert "Invalid overwrite format" not in result.output

    def test_run_with_invalid_overwrite_format_cli(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test run command with invalid overwrite format via CLI."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: ""

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "run",
                "--config_path",
                str(config_path),
                "-o",
                "invalid_no_equals",
            ],
        )
        assert result.exit_code != 0
        assert "Invalid overwrite format" in result.output

    def test_apply_overwrites_bundle_not_found(self) -> None:
        """Test that overwrite raises error when bundle doesn't exist."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {},
            "check_bundles": [
                {
                    "name": "existing_bundle",
                    "checks": [],
                },
            ],
        }

        # Apply overwrite for non-existent bundle
        with pytest.raises(click.BadParameter, match=r"Bundle 'nonexistent' not found"):
            _apply_overwrites_to_dict(config_dict, [("check_bundles.nonexistent.filters.date", "2023-01-01")])

    def test_apply_overwrites_bundle_not_found_shows_available(self) -> None:
        """Test that error message shows available bundles."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {},
            "check_bundles": [
                {"name": "bundle_a", "checks": []},
                {"name": "bundle_b", "checks": []},
            ],
        }

        with pytest.raises(click.BadParameter, match=r"Available bundles: bundle_a, bundle_b"):
            _apply_overwrites_to_dict(config_dict, [("check_bundles.unknown.table", "x")])

    def test_apply_overwrites_check_index_out_of_range(self) -> None:
        """Test that overwrite raises error when check index is out of range."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {},
            "check_bundles": [
                {
                    "name": "my_bundle",
                    "checks": [
                        {"check_type": "CountCheck", "table": "t", "check_column": "c"},
                    ],
                },
            ],
        }

        # Bundle has 1 check, index 5 is out of range
        with pytest.raises(click.BadParameter, match=r"Check index 5 out of range.*has 1 checks"):
            _apply_overwrites_to_dict(config_dict, [("check_bundles.my_bundle.5.table", "other")])

    def test_apply_overwrites_check_index_zero_on_empty_bundle(self) -> None:
        """Test that overwrite raises error when bundle has no checks."""
        config_dict = {
            "name": "test_config",
            "database_setup": "",
            "database_accessor": "memory",
            "defaults": {},
            "check_bundles": [
                {
                    "name": "empty_bundle",
                    "checks": [],
                },
            ],
        }

        with pytest.raises(click.BadParameter, match=r"Check index 0 out of range.*has 0 checks"):
            _apply_overwrites_to_dict(config_dict, [("check_bundles.empty_bundle.0.table", "x")])

    def test_cli_error_bundle_not_found(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test CLI shows error when bundle doesn't exist."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: memory

            defaults:
              monitor_only: true

            check_bundles:
              - name: orders
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            ["print", "--config_path", str(config_path), "-o", "check_bundles.nonexistent.table=x"],
        )
        assert result.exit_code != 0
        assert "Bundle 'nonexistent' not found" in result.output
        assert "Available bundles: orders" in result.output

    def test_cli_error_check_index_out_of_range(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test CLI shows error when check index is out of range."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: memory

            defaults:
              monitor_only: true

            check_bundles:
              - name: orders
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            ["print", "--config_path", str(config_path), "-o", "check_bundles.orders.5.table=x"],
        )
        assert result.exit_code != 0
        assert "Check index 5 out of range" in result.output
        assert "has 1 checks" in result.output

    def test_cli_error_bundle_not_found_run_command(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test run command shows error when bundle doesn't exist."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: memory

            defaults:
              monitor_only: true

            check_bundles:
              - name: my_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            ["run", "--config_path", str(config_path), "-o", "check_bundles.wrong_name.filters.date=today"],
        )
        assert result.exit_code != 0
        assert "Bundle 'wrong_name' not found" in result.output
        assert "Available bundles: my_bundle" in result.output


class TestDatabaseSetupVariables:
    """Tests for the database_setup_variable functionality."""

    def test_parse_variables_valid(self) -> None:
        """Test parsing valid variable arguments."""
        variables = ("PROJECT_ID=my-project", "DATASET=analytics")
        result = _parse_variables(variables)
        assert result == {"PROJECT_ID": "my-project", "DATASET": "analytics"}

    def test_parse_variables_with_spaces(self) -> None:
        """Test parsing variables with spaces around key/value."""
        variables = ("PROJECT_ID = my-project",)
        result = _parse_variables(variables)
        assert result == {"PROJECT_ID": "my-project"}

    def test_parse_variables_value_with_equals(self) -> None:
        """Test parsing variables where value contains equals sign."""
        variables = ("CONFIG=key=value",)
        result = _parse_variables(variables)
        assert result == {"CONFIG": "key=value"}

    def test_parse_variables_invalid_format(self) -> None:
        """Test parsing invalid variable format raises error."""
        variables = ("invalid_no_equals",)
        with pytest.raises(click.BadParameter, match="Invalid variable format"):
            _parse_variables(variables)

    def test_parse_variables_empty(self) -> None:
        """Test parsing empty variables."""
        result = _parse_variables(())
        assert result == {}

    def test_run_help_shows_database_setup_variable_option(self, runner: CliRunner) -> None:
        """Test that run command help shows database_setup_variable option."""
        result = runner.invoke(cli, ["run", "--help"])
        assert result.exit_code == 0
        assert "--database_setup_variable" in result.output or "-dsv" in result.output

    def test_print_help_shows_database_setup_variable_option(self, runner: CliRunner) -> None:
        """Test that print command help shows database_setup_variable option."""
        result = runner.invoke(cli, ["print", "--help"])
        assert result.exit_code == 0
        assert "--database_setup_variable" in result.output or "-dsv" in result.output

    def test_print_with_database_setup_variable(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test print command with database_setup_variable option."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: |
              INSTALL bigquery;
              LOAD bigquery;
              ATTACH 'project=${PROJECT_ID}' AS bq (TYPE bigquery);
            database_accessor: bq

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
                "--format",
                "json",
                "-dsv",
                "PROJECT_ID=my-gcp-project",
            ],
        )
        assert result.exit_code == 0
        # Verify the variable was substituted
        assert "my-gcp-project" in result.output
        assert "${PROJECT_ID}" not in result.output
        # Parse JSON and verify
        parsed = json.loads(result.output)
        assert "project=my-gcp-project" in parsed["database_setup"]

    def test_print_with_multiple_database_setup_variables(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test print command with multiple database_setup_variable options."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: |
              ATTACH 'project=${PROJECT_ID}' AS ${ACCESSOR} (TYPE bigquery);
            database_accessor: bq

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
                "--format",
                "json",
                "-dsv",
                "PROJECT_ID=prod-project",
                "-dsv",
                "ACCESSOR=bigquery_db",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "project=prod-project" in parsed["database_setup"]
        assert "bigquery_db" in parsed["database_setup"]

    def test_print_with_undefined_variable_shows_error(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test print command with undefined variable shows error."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: |
              ATTACH 'project=${PROJECT_ID}' AS bq;
            database_accessor: bq

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        # Don't provide the required variable
        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
            ],
        )
        assert result.exit_code != 0
        assert "PROJECT_ID" in result.output
        assert "not defined" in result.output

    def test_print_without_variables_when_none_needed(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test print command works without variables when none are referenced."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: |
              ATTACH 'mydb.duckdb' AS mydb;
            database_accessor: mydb

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
                "--format",
                "json",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "mydb.duckdb" in parsed["database_setup"]

    def test_run_with_invalid_variable_format(self, runner: CliRunner, tmp_path: Path) -> None:
        """Test run command with invalid variable format via CLI."""
        config_yaml = dedent("""\
            name: test_config
            database_setup: ""
            database_accessor: ""

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "run",
                "--config_path",
                str(config_path),
                "-dsv",
                "invalid_no_equals",
            ],
        )
        assert result.exit_code != 0
        assert "Invalid variable format" in result.output

    def test_parse_env_variables_valid(self) -> None:
        """Test parsing valid environment variable string."""
        result = _parse_env_variables("PROJECT_ID=my-project,DATASET=analytics")
        assert result == {"PROJECT_ID": "my-project", "DATASET": "analytics"}

    def test_parse_env_variables_with_spaces(self) -> None:
        """Test parsing env variables with spaces."""
        result = _parse_env_variables("PROJECT_ID = my-project , DATASET = analytics")
        assert result == {"PROJECT_ID": "my-project", "DATASET": "analytics"}

    def test_parse_env_variables_empty_string(self) -> None:
        """Test parsing empty environment variable string."""
        result = _parse_env_variables("")
        assert result == {}

    def test_parse_env_variables_whitespace_only(self) -> None:
        """Test parsing whitespace-only environment variable string."""
        result = _parse_env_variables("   ")
        assert result == {}

    def test_parse_env_variables_invalid_format(self) -> None:
        """Test parsing invalid env variable format raises error."""
        with pytest.raises(click.ClickException, match="Invalid variable format"):
            _parse_env_variables("PROJECT_ID=value,invalid_no_equals")

    def test_parse_env_variables_trailing_comma(self) -> None:
        """Test parsing env variables with trailing comma."""
        result = _parse_env_variables("PROJECT_ID=value,")
        assert result == {"PROJECT_ID": "value"}

    def test_get_variables_with_env_only_cli(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting variables from CLI only (no env var)."""
        monkeypatch.delenv(DATABASE_SETUP_VARIABLES_ENV, raising=False)
        result = _get_variables_with_env(("PROJECT_ID=cli-value",))
        assert result == {"PROJECT_ID": "cli-value"}

    def test_get_variables_with_env_only_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting variables from environment only (no CLI)."""
        monkeypatch.setenv(DATABASE_SETUP_VARIABLES_ENV, "PROJECT_ID=env-value")
        result = _get_variables_with_env(())
        assert result == {"PROJECT_ID": "env-value"}

    def test_get_variables_with_env_cli_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that CLI variables override environment variables."""
        monkeypatch.setenv(DATABASE_SETUP_VARIABLES_ENV, "PROJECT_ID=env-value,DATASET=env-dataset")
        result = _get_variables_with_env(("PROJECT_ID=cli-value",))
        assert result == {"PROJECT_ID": "cli-value", "DATASET": "env-dataset"}

    def test_get_variables_with_env_combined(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test combining env and CLI variables."""
        monkeypatch.setenv(DATABASE_SETUP_VARIABLES_ENV, "PROJECT_ID=env-project")
        result = _get_variables_with_env(("DATASET=cli-dataset",))
        assert result == {"PROJECT_ID": "env-project", "DATASET": "cli-dataset"}

    def test_print_with_env_variable(self, runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test print command with environment variable."""
        monkeypatch.setenv(DATABASE_SETUP_VARIABLES_ENV, "PROJECT_ID=env-gcp-project")
        config_yaml = dedent("""\
            name: test_config
            database_setup: |
              ATTACH 'project=${PROJECT_ID}' AS bq;
            database_accessor: bq

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
                "--format",
                "json",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert "project=env-gcp-project" in parsed["database_setup"]

    def test_print_cli_overrides_env_variable(
        self,
        runner: CliRunner,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that CLI variable overrides environment variable."""
        monkeypatch.setenv(DATABASE_SETUP_VARIABLES_ENV, "PROJECT_ID=env-project")
        config_yaml = dedent("""\
            name: test_config
            database_setup: |
              ATTACH 'project=${PROJECT_ID}' AS bq;
            database_accessor: bq

            defaults:
              monitor_only: true

            check_bundles:
              - name: test_bundle
                checks:
                  - check_type: CountCheck
                    table: test_table
                    check_column: id
                    lower_threshold: 0
                    upper_threshold: 100
            """)
        config_path = tmp_path / "test_config.yaml"
        config_path.write_text(config_yaml)

        result = runner.invoke(
            cli,
            [
                "print",
                "--config_path",
                str(config_path),
                "--format",
                "json",
                "-dsv",
                "PROJECT_ID=cli-project",
            ],
        )
        assert result.exit_code == 0
        parsed = json.loads(result.output)
        # CLI should override env
        assert "project=cli-project" in parsed["database_setup"]
        assert "env-project" not in parsed["database_setup"]
