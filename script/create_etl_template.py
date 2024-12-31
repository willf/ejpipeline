import click
from pathlib import Path


def create_etl_template(file_name):
    # Extract class name and ETL name from the file name
    class_name = "".join(word.capitalize() for word in file_name.split("_")) + "ETL"
    etl_name = file_name.replace("_", " ")

    # Template for the ETL class
    template = f"""
from data_pipeline.etl.base import BaseETL
from data_pipeline import utils
import requests

class {class_name}(BaseETL):
    def __init__(self):
        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(cls):
        return "{etl_name}"

    def extract(self):
        self.logger.info(f"Extracting data from {{self.etl_name()}}")
        # Add extraction logic here
        destination = self.save_source_path("some_file.csv")
        self.logger.info(
            f"Downloaded {{self.etl_name()}} data files to {{destination}}"
        )

    def transform(self):
        # self.logger.info(f"Transforming data for {{self.etl_name()}}")
        # Add transformation logic here
        pass

    def load(self):
        # self.logger.info(f"Loading data for {{self.etl_name()}}")
        # Add loading logic here
        pass

if __name__ == "__main__":
    etl = {class_name}()
    etl.run(use_cache=True)
"""
    return template


@click.command()
@click.option(
    "--source_metadata_template",
    default=None,
    help="File name of the source metadata template",
)
@click.argument(
    "module_path",
    type=click.Path(),
)
@click.argument(
    "metadata_target_directory",
    type=click.Path(),
)
def main(module_path, metadata_target_directory, source_metadata_template):
    # Create the ETL module directory
    module_path = Path(module_path)
    module_path.mkdir(parents=True, exist_ok=True)

    # Create the ETL class template
    file_name = module_path.name
    etl_template = create_etl_template(file_name)

    # Write the ETL class template to
    etl_file = module_path / "etl.py"
    etl_file.write_text(etl_template)
    print(f"Created ETL function at {etl_file}")

    # Create the metadata directory
    metadata_directory = Path(metadata_target_directory) / file_name
    metadata_directory.mkdir(parents=True, exist_ok=True)

    source_template_file = source_metadata_template
    if not source_template_file:
        source_template_file = "metadata-edgi.toml"

    # Create the source metadata template
    target_metadata_file = metadata_directory / f"metadata-{file_name}.toml"
    target_metadata_file.touch()
    # copy the contents of the source metadata template file to the new source metadata file
    with open(source_template_file, "r") as f:
        target_metadata_file.write_text(f.read())
    print(f"Created metadata file at {target_metadata_file}")


if __name__ == "__main__":
    main()
