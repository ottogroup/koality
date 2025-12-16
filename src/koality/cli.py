"""CLI for koality."""

from pathlib import Path

import click
from pydantic_yaml import parse_yaml_raw_as

from koality.executor import CheckExecutor
from koality.models import Config
from koality.utils import parse_arg


@click.command(context_settings={"ignore_unknown_options": True, "allow_extra_args": True})
@click.option("--config_path", required=True, type=click.Path(exists=True, dir_okay=False))
@click.pass_context
def cli(ctx: click.Context, config_path: Path) -> None:
    """
    CLI for koality. Besides config_path and project_id, additional arguments
    can be provided that will overrule the global default configuration.

    Args:
        ctx: Context of command line invocation (contains extra args)
        config_path: Path to koality configuration file
    """
    kwargs = {ctx.args[i].lstrip("-"): ctx.args[i + 1] for i in range(0, len(ctx.args), 2)}

    for key, val in kwargs.items():
        kwargs[key] = parse_arg(val)

    config = parse_yaml_raw_as(Config, Path(config_path).read_text())

    check_executor = CheckExecutor(config=config, **kwargs)
    _ = check_executor()
