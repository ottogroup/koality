"""Unit tests for CLI commands."""

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from koality.cli import cli

pytestmark = pytest.mark.unit

VALID_CONFIG = """
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
"""

INVALID_CONFIG = """
name: missing_required_fields
"""


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
