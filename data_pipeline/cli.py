# Create a CLI for the data pipeline. Run the following command in the terminal:
#
# python -m data_pipeline.cli run_all

import click
from data_pipeline.etl.base import BaseETL


@click.command()
@click.option("--force", is_flag=True, default=False, help="Force the ETL to rerun")
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Simulate the ETL run without making any changes",
)
@click.argument("etl_name", required=False, nargs=-1)
def main(force, dry_run, etl_name):
    use_cache = not force
    BaseETL().run_all(use_cache=use_cache, dry_run=dry_run, etl_names=etl_name)


if __name__ == "__main__":
    main()
