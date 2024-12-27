# Environmental Justice Data Pipeline (EJPipeline)

[![Python tests](https://github.com/willf/ejpipeline/actions/workflows/test.yml/badge.svg)](https://github.com/willf/ejpipeline/actions/workflows/test.yml)

This repository contains the code for the Environmental Justice Data Pipeline (EJPipeline) project. The EJPipeline is a data processing pipeline collects data from various sources relevant to environmental justice and processes it to make it more accessible to researchers and the public. The pipeline is designed to be modular and extensible, allowing for the addition of new data sources and processing steps. We also aim to minimize the Python dependencies required to run the pipeline, so that it can be run on a wide variety of systems. Further, we aim to use previous open source work as much as possible, and to contribute back to the open source community.

## Installation

The current way to install the EJPipeline to clone the repository, and run it using `uv`. For example,
if you have `gh` installed, you can run:

```bash
gh repo clone willf/ejpipeline
cd ejpipeline
```

## Use

To run the pipeline, you can use `uv` to run the `etl/base.py` script from the ejpipeline directory. For example:

```bash
PYTHONPATH=`pwd`:$PYTHONPATH uv run data_pipeline/etl/base.py
```

Use `--force` to force the pipeline to run, even if the data is up to date.

You will likely need to install playwright browsers the first time.

`uv run playwright install`

## Development

ETL modules are located in the `data_pipeline/etl` directory. Each module should be a subclass of `BaseETL` and should implement the `extract`, `transform`, and `load` methods as necessary, as well as an `already_etled` method to check if the data is already up to date. See, for example, the `data_pipeline/etl/calenviroscreen/` module.
