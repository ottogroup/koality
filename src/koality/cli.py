"""Command-line interface for Koality.

This module provides the CLI for running, validating, and inspecting
Koality data quality check configurations.

Commands:
    run: Execute data quality checks from a configuration file.
    validate: Validate a configuration file without executing checks.
    print: Print the resolved configuration in various formats.

Example:
    $ koality run --config_path checks.yaml
    $ koality validate --config_path checks.yaml
    $ koality print --config_path checks.yaml --format json

"""

import os
from pathlib import Path
from typing import Any

import click
import yaml
from pydantic import ValidationError

from koality.executor import CheckExecutor
from koality.models import Config
from koality.utils import substitute_variables

DATABASE_SETUP_VARIABLES_ENV = "DATABASE_SETUP_VARIABLES"


@click.group()
def cli() -> None:
    """Koality - Data quality monitoring CLI.

    Koality provides commands to run, validate, and inspect data quality
    check configurations. Use --help on any command for more details.
    """


@cli.command()
@click.option(
    "--config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the YAML configuration file.",
)
@click.option(
    "--overwrites",
    "-o",
    multiple=True,
    help="Override filter values in defaults. Format: filter_name=value. "
    "Overwrites propagate to all checks. Can be used multiple times. "
    "Example: -o partition_date=2023-01-01 -o shop_id=SHOP02",
)
@click.option(
    "--database_setup_variable",
    "-dsv",
    multiple=True,
    help="Set variables for database_setup substitution. Format: VAR_NAME=value. "
    "Variables are substituted in database_setup using ${VAR_NAME} syntax. "
    "Can be used multiple times. Also reads from DATABASE_SETUP_VARIABLES env var "
    "(format: VAR1=value1,VAR2=value2). CLI options override env vars. "
    "Example: -dsv PROJECT_ID=my-gcp-project",
)
def run(
    config_path: Path,
    overwrites: tuple[str, ...],
    database_setup_variable: tuple[str, ...],
) -> None:
    """Run koality checks from a configuration file.

    Executes all data quality checks defined in the configuration file.
    Filter values can be overridden using --overwrites (-o) options.
    Overwrites are applied to defaults before validation, so they
    propagate to all checks automatically.

    Database setup variables can be provided using --database_setup_variable (-dsv)
    to substitute ${VAR_NAME} placeholders in the database_setup SQL. Variables
    can also be set via the DATABASE_SETUP_VARIABLES environment variable using
    comma-separated VAR=value pairs.

    Examples:
        koality run --config_path checks.yaml

        koality run --config_path checks.yaml -o partition_date=2023-01-01

        koality run --config_path checks.yaml -dsv PROJECT_ID=my-gcp-project

        koality run --config_path checks.yaml -dsv PROJECT_ID=prod -dsv DATASET=analytics

        DATABASE_SETUP_VARIABLES="PROJECT_ID=my-project,DATASET=prod" koality run --config_path checks.yaml

    """
    variables = _get_variables_with_env(database_setup_variable)
    config = _load_config_with_overwrites(config_path, overwrites, variables)
    check_executor = CheckExecutor(config=config)
    _ = check_executor()


def _parse_overwrites(overwrites: tuple[str, ...]) -> list[tuple[str, str]]:
    """Parse overwrite arguments into a list of (path, value) tuples.

    Args:
        overwrites: Tuple of "path=value" strings.

    Returns:
        List of (path, value) tuples preserving order.

    Raises:
        click.BadParameter: If an overwrite is not in path=value format.

    """
    result = []
    for overwrite in overwrites:
        if "=" not in overwrite:
            msg = f"Invalid overwrite format: '{overwrite}'. Expected format: path=value"
            raise click.BadParameter(msg)
        path, value = overwrite.split("=", 1)
        result.append((path.strip(), value.strip()))
    return result


def _parse_variables(variables: tuple[str, ...] | list[str]) -> dict[str, str]:
    """Parse variable arguments into a dict.

    Args:
        variables: Tuple or list of "VAR_NAME=value" strings.

    Returns:
        Dict of variable name -> value mappings.

    Raises:
        click.BadParameter: If a variable is not in VAR_NAME=value format.

    """
    result = {}
    for var in variables:
        if "=" not in var:
            msg = f"Invalid variable format: '{var}'. Expected format: VAR_NAME=value"
            raise click.BadParameter(msg)
        name, value = var.split("=", 1)
        result[name.strip()] = value.strip()
    return result


def _parse_env_variables(env_value: str) -> dict[str, str]:
    """Parse DATABASE_SETUP_VARIABLES environment variable.

    Args:
        env_value: Comma-separated "VAR_NAME=value" pairs.

    Returns:
        Dict of variable name -> value mappings.

    Raises:
        click.ClickException: If a variable is not in VAR_NAME=value format.

    """
    if not env_value.strip():
        return {}

    result = {}
    for var in env_value.split(","):
        var_stripped = var.strip()
        if not var_stripped:
            continue
        if "=" not in var_stripped:
            msg = (
                f"Invalid variable format in {DATABASE_SETUP_VARIABLES_ENV}: '{var_stripped}'. "
                f"Expected format: VAR1=value1,VAR2=value2"
            )
            raise click.ClickException(msg)
        name, value = var_stripped.split("=", 1)
        result[name.strip()] = value.strip()
    return result


def _get_variables_with_env(cli_variables: tuple[str, ...]) -> dict[str, str]:
    """Get variables from both environment and CLI, with CLI taking precedence.

    Args:
        cli_variables: Tuple of "VAR_NAME=value" strings from CLI.

    Returns:
        Dict of variable name -> value mappings, with CLI overriding env vars.

    """
    # Start with environment variables
    env_value = os.environ.get(DATABASE_SETUP_VARIABLES_ENV, "")
    variables = _parse_env_variables(env_value)

    # CLI variables override environment variables
    cli_vars = _parse_variables(cli_variables)
    variables.update(cli_vars)

    return variables


def _apply_overwrites_to_dict(config_dict: dict[str, Any], overwrites: list[tuple[str, str]]) -> None:
    """Apply overwrite values to the raw config dict before validation.

    Supports flexible path-based overwrites:
    - Simple field: identifier_format=filter_name -> defaults.identifier_format
    - Filter value: filters.partition_date=2023-01-01 -> defaults.filters.partition_date.value
    - Explicit defaults: defaults.identifier_format=column_name -> defaults.identifier_format
    - Bundle default: check_bundles.bundle_name.identifier_format=x -> bundle.defaults.identifier_format
    - Bundle filter: check_bundles.bundle_name.filters.date=x -> bundle.defaults.filters.date.value
    - Check field: check_bundles.bundle_name.0.table=x -> bundle.checks[0].table
    - Check filter: check_bundles.bundle_name.0.filters.date=x -> bundle.checks[0].filters.date.value

    Args:
        config_dict: The raw configuration dictionary (parsed from YAML).
        overwrites: List of (path, value) tuples.

    """
    # Build a lookup for bundle names to their indices
    bundle_lookup = {}
    for idx, bundle in enumerate(config_dict.get("check_bundles", [])):
        bundle_name = bundle.get("name")
        if bundle_name:
            bundle_lookup[bundle_name] = idx

    for path, value in overwrites:
        parts = path.split(".")
        _apply_single_overwrite(config_dict, parts, value, bundle_lookup)


def _apply_single_overwrite(
    config_dict: dict[str, Any],
    parts: list[str],
    value: str,
    bundle_lookup: dict[str, int],
) -> None:
    """Apply a single overwrite to the config dict.

    Args:
        config_dict: The raw configuration dictionary.
        parts: Path components (e.g., ["filters", "partition_date"]).
        value: The value to set.
        bundle_lookup: Mapping of bundle names to their indices.

    """
    min_bundle_path_len = 2  # check_bundles.<bundle_name>

    # Handle explicit check_bundles prefix
    if parts[0] == "check_bundles" and len(parts) >= min_bundle_path_len:
        bundle_name = parts[1]
        if bundle_name not in bundle_lookup:
            available = ", ".join(bundle_lookup.keys()) if bundle_lookup else "none"
            msg = f"Bundle '{bundle_name}' not found. Available bundles: {available}"
            raise click.BadParameter(msg)

        bundle_idx = bundle_lookup[bundle_name]
        bundle = config_dict["check_bundles"][bundle_idx]
        remaining = parts[2:]

        # Check if next part is a check index
        if remaining and remaining[0].isdigit():
            check_idx = int(remaining[0])
            num_checks = len(bundle.get("checks", []))
            if check_idx >= num_checks:
                msg = f"Check index {check_idx} out of range for bundle '{bundle_name}' (has {num_checks} checks)"
                raise click.BadParameter(msg)
            check = bundle["checks"][check_idx]
            _set_value_at_path(check, remaining[1:], value, is_check_level=True)
        else:
            # Bundle-level defaults
            if "defaults" not in bundle:
                bundle["defaults"] = {}
            _set_value_at_path(bundle["defaults"], remaining, value, is_check_level=False)
        return

    # Handle explicit defaults prefix (optional but supported)
    if parts[0] == "defaults":
        if "defaults" not in config_dict:
            config_dict["defaults"] = {}
        _set_value_at_path(config_dict["defaults"], parts[1:], value, is_check_level=False)
        return

    # Default: apply to global defaults (implicit)
    if "defaults" not in config_dict:
        config_dict["defaults"] = {}
    _set_value_at_path(config_dict["defaults"], parts, value, is_check_level=False)


def _set_value_at_path(target: dict[str, Any], parts: list[str], value: str, *, is_check_level: bool) -> None:
    """Set a value at a path within a target dict.

    Args:
        target: The target dictionary (defaults or check).
        parts: Remaining path components.
        value: The value to set.
        is_check_level: Whether we're setting at check level (affects filter handling).

    """
    if not parts:
        return

    min_filter_path_len = 2

    # Handle explicit filters path (e.g., filters.partition_date or filters.partition_date.column)
    if parts[0] == "filters" and len(parts) >= min_filter_path_len:
        filter_name = parts[1]
        # Determine which field to set (default to "value" if not specified)
        field_name = parts[2] if len(parts) > min_filter_path_len else "value"
        _set_filter_field(target, filter_name, field_name, value)
        return

    if len(parts) == 1:
        _set_single_field(target, parts[0], value, is_check_level=is_check_level)
        return

    # Nested path - navigate/create intermediate dicts
    if parts[0] not in target:
        target[parts[0]] = {}
    if isinstance(target[parts[0]], dict):
        _set_value_at_path(target[parts[0]], parts[1:], value, is_check_level=is_check_level)


def _set_filter_field(target: dict[str, Any], filter_name: str, field_name: str, value: str) -> None:
    """Set a specific field on a filter within the target dict.

    Args:
        target: The target dictionary containing filters.
        filter_name: Name of the filter to update.
        field_name: Name of the field to set (e.g., "value", "column", "operator").
        value: The new value for the field.

    """
    if "filters" not in target:
        target["filters"] = {}
    filters = target["filters"]

    if filter_name not in filters:
        # Filter doesn't exist - create it with the specified field
        # This handles cases where the filter is inherited from defaults
        # and we need to override it at check/bundle level
        filters[filter_name] = {field_name: _convert_filter_field_value(field_name, value)}
        return

    filter_config = filters[filter_name]
    if isinstance(filter_config, dict):
        filter_config[field_name] = _convert_filter_field_value(field_name, value)
    elif field_name == "value":
        # Shorthand filter - replace directly (only for value field)
        filters[filter_name] = value
    else:
        # Shorthand filter but trying to set non-value field - convert to dict
        filters[filter_name] = {"value": filter_config, field_name: _convert_filter_field_value(field_name, value)}


def _convert_filter_field_value(field_name: str, value: str) -> str | bool:
    """Convert filter field value to appropriate type.

    Args:
        field_name: Name of the filter field.
        value: The string value to convert.

    Returns:
        Converted value.

    """
    # Boolean fields in filter config
    if field_name == "parse_as_date":
        return value.lower() in ("true", "1", "yes")
    return value


def _set_single_field(target: dict[str, Any], field_name: str, value: str, *, is_check_level: bool) -> None:
    """Set a single field value, checking for filter name backward compatibility.

    Args:
        target: The target dictionary.
        field_name: Name of the field to set.
        value: The value to set.
        is_check_level: Whether we're setting at check level.

    """
    # Check if this is a filter name (backward compatibility)
    filters = target.get("filters", {})
    if field_name in filters:
        filter_config = filters[field_name]
        if isinstance(filter_config, dict):
            filter_config["value"] = value
        else:
            filters[field_name] = value
    elif is_check_level and field_name not in _KNOWN_CHECK_FIELDS:
        # At check level, assume unknown fields are filter names
        # (the filter is likely inherited from defaults)
        _set_filter_field(target, field_name, "value", value)
    else:
        # Direct field assignment (e.g., identifier_format, monitor_only, table)
        target[field_name] = _convert_value(field_name, value)


# Known check-level fields that should be set directly, not treated as filters
_KNOWN_CHECK_FIELDS = frozenset(
    {
        "check_type",
        "table",
        "left_table",
        "right_table",
        "check_column",
        "lower_threshold",
        "upper_threshold",
        "monitor_only",
        "result_table",
        "log_path",
        "identifier_format",
        "date_info",
        "extra_info",
        "regex_to_match",
        "value_set",
        "distinct",
        "max_or_min",
        "join_columns",
        "join_columns_left",
        "join_columns_right",
        "rolling_days",
        "interval_days",
        "how",
        "iqr_factor",
    },
)


def _convert_value(field_name: str, value: str) -> str | bool | int | float:
    """Convert string value to appropriate type based on field name.

    Args:
        field_name: The name of the field being set.
        value: The string value to convert.

    Returns:
        Converted value (bool, int, float, or original string).

    """
    # Boolean fields
    if field_name in ("monitor_only", "fail_if_no_rows"):
        return value.lower() in ("true", "1", "yes")

    # Try numeric conversion
    if value.isdigit():
        return int(value)
    try:
        return float(value)
    except ValueError:
        pass

    return value


def _load_config_with_overwrites(
    config_path: Path,
    overwrites: tuple[str, ...],
    variables: dict[str, str] | None = None,
) -> Config:
    """Load and parse config file, applying overwrites and variables before validation.

    Args:
        config_path: Path to the YAML configuration file.
        overwrites: Tuple of "key=value" overwrite strings.
        variables: Dict of variable name -> value for ${VAR} substitution
            in database_setup.

    Returns:
        Parsed and validated Config object with overwrites and variables applied.

    """
    config_dict = yaml.safe_load(Path(config_path).read_text())

    if overwrites:
        overwrite_dict = _parse_overwrites(overwrites)
        _apply_overwrites_to_dict(config_dict, overwrite_dict)

    # Apply variable substitution to database_setup
    if "database_setup" in config_dict:
        try:
            config_dict["database_setup"] = substitute_variables(
                config_dict["database_setup"],
                variables or {},
            )
        except ValueError as e:
            raise click.ClickException(str(e)) from None

    return Config.model_validate(config_dict)


@cli.command()
@click.option(
    "--config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the YAML configuration file.",
)
def validate(config_path: Path) -> None:
    """Validate a koality configuration file.

    Parses and validates the configuration file against the Koality schema
    without executing any checks. Useful for CI/CD pipelines and debugging.

    Exit codes:

        0: Configuration is valid.

        1: Configuration is invalid.

    Examples:
        koality validate --config_path checks.yaml

    """
    try:
        config_dict = yaml.safe_load(Path(config_path).read_text())
        Config.model_validate(config_dict)
        click.echo(f"Configuration '{config_path}' is valid.")
    except ValidationError as e:
        click.echo(f"Configuration '{config_path}' is invalid:\n{e}", err=True)
        raise SystemExit(1) from None


@cli.command(name="print")
@click.option(
    "--config_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the YAML configuration file.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["model", "yaml", "json"]),
    default="yaml",
    help="Output format: 'model' (Pydantic repr), 'yaml', or 'json'.",
)
@click.option(
    "--indent",
    default=2,
    type=int,
    help="Indentation level for yaml/json output.",
)
@click.option(
    "--overwrites",
    "-o",
    multiple=True,
    help="Override filter values in defaults. Format: filter_name=value. "
    "Overwrites propagate to all checks. Can be used multiple times. "
    "Example: -o partition_date=2023-01-01 -o shop_id=SHOP02",
)
@click.option(
    "--database_setup_variable",
    "-dsv",
    multiple=True,
    help="Set variables for database_setup substitution. Format: VAR_NAME=value. "
    "Variables are substituted in database_setup using ${VAR_NAME} syntax. "
    "Can be used multiple times. Also reads from DATABASE_SETUP_VARIABLES env var "
    "(format: VAR1=value1,VAR2=value2). CLI options override env vars. "
    "Example: -dsv PROJECT_ID=my-gcp-project",
)
def print_config(
    config_path: Path,
    output_format: str,
    indent: int,
    overwrites: tuple[str, ...],
    database_setup_variable: tuple[str, ...],
) -> None:
    """Print the resolved koality configuration.

    Displays the fully resolved configuration after default propagation.
    This shows the effective configuration that would be used during execution.
    Filter values can be overridden using --overwrites (-o) options.
    Overwrites are applied to defaults before validation, so they
    propagate to all checks automatically.

    Database setup variables can be provided using --database_setup_variable (-dsv)
    to substitute ${VAR_NAME} placeholders in the database_setup SQL. Variables
    can also be set via the DATABASE_SETUP_VARIABLES environment variable using
    comma-separated VAR=value pairs.

    Output formats:

        model: Pydantic model representation (Python repr).

        yaml: YAML formatted output (default).

        json: JSON formatted output.

    Examples:
        koality print --config_path checks.yaml

        koality print --config_path checks.yaml --format json

        koality print --config_path checks.yaml --format yaml --indent 4

        koality print --config_path checks.yaml -o partition_date=2023-01-01

        koality print --config_path checks.yaml -dsv PROJECT_ID=my-gcp-project

        DATABASE_SETUP_VARIABLES="PROJECT_ID=my-project" koality print --config_path checks.yaml

    """
    variables = _get_variables_with_env(database_setup_variable)
    try:
        config = _load_config_with_overwrites(config_path, overwrites, variables)
    except ValidationError as e:
        click.echo(f"Configuration '{config_path}' is invalid:\n{e}", err=True)
        raise SystemExit(1) from None

    if output_format == "model":
        click.echo(config)
    elif output_format == "json":
        click.echo(config.model_dump_json(indent=indent))
    else:  # yaml
        click.echo(_dump_yaml(config.model_dump(), indent=indent))


class _LiteralBlockDumper(yaml.SafeDumper):
    """Custom YAML dumper that uses literal block style for multiline strings."""


def _literal_str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    """Represent multiline strings using literal block style (|)."""
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_LiteralBlockDumper.add_representer(str, _literal_str_representer)


def _dump_yaml(data: dict, *, indent: int = 2) -> str:
    """Dump data to YAML with proper multiline string handling."""
    return yaml.dump(data, Dumper=_LiteralBlockDumper, default_flow_style=False, sort_keys=False, indent=indent)
