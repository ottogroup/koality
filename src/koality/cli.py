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

from pathlib import Path

import click
import yaml
from pydantic import ValidationError
from pydantic_yaml import parse_yaml_raw_as

from koality.executor import CheckExecutor
from koality.models import Config


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
def run(config_path: Path) -> None:
    """Run koality checks from a configuration file.

    Executes all data quality checks defined in the configuration file.
    Additional arguments can be provided to override global defaults.

    Examples:
        koality run --config_path checks.yaml

    """
    config = parse_yaml_raw_as(Config, Path(config_path).read_text())
    check_executor = CheckExecutor(config=config)
    _ = check_executor()


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
        parse_yaml_raw_as(Config, Path(config_path).read_text())
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
def print_config(config_path: Path, output_format: str, indent: int) -> None:
    """Print the resolved koality configuration.

    Displays the fully resolved configuration after default propagation.
    This shows the effective configuration that would be used during execution.

    Output formats:

        model: Pydantic model representation (Python repr).

        yaml: YAML formatted output (default).

        json: JSON formatted output.

    Examples:
        koality print --config_path checks.yaml

        koality print --config_path checks.yaml --format json

        koality print --config_path checks.yaml --format yaml --indent 4

    """
    try:
        config = parse_yaml_raw_as(Config, Path(config_path).read_text())
    except ValidationError as e:
        click.echo(f"Configuration '{config_path}' is invalid:\n{e}", err=True)
        raise SystemExit(1) from None

    if output_format == "model":
        click.echo(config)
    elif output_format == "json":
        click.echo(config.model_dump_json(indent=indent))
    else:  # yaml
        click.echo(yaml.dump(config.model_dump(), default_flow_style=False, sort_keys=False, indent=indent))
