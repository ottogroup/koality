"""CLI for koality."""

import click
from google.cloud import bigquery as bq

from src.koality.executor import CheckExecutor
from src.koality.utils import parse_arg


@click.command(context_settings={"ignore_unknown_options": True, "allow_extra_args": True})
@click.option("--config_path", required=True)
@click.option("--project_id", required=True)
@click.pass_context
def cli(ctx, config_path: str, project_id: str) -> None:
    """
    CLI for koality. Besides config_path and project_id, additional arguments
    can be provided that will overrule the global default configuration.

    Args:
        ctx: Context of command line invocation (contains extra args)
        config_path: Path to koality configuration file
        project_id: GCP project ID
    """
    kwargs = {ctx.args[i].lstrip("-"): ctx.args[i + 1] for i in range(0, len(ctx.args), 2)}

    for key, val in kwargs.items():
        kwargs[key] = parse_arg(val)

    bq_client = bq.Client(project_id)

    check_executor = CheckExecutor(config_path=config_path, bq_client=bq_client, **kwargs)
    _ = check_executor()
