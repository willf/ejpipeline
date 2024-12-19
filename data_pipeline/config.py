import pathlib

# assume the sink location is at the same level as the level _above_ the data_pipeline directory

DATA_DIR = pathlib.Path(__file__).parent.parent.parent / "ejpipeline" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
