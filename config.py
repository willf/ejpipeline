from dynaconf import Dynaconf
import pathlib
import data_pipeline

settings = Dynaconf(
    envvar_prefix="EDGI",
    settings_files=["settings.toml", ".secrets.toml"],
)

if settings.get("DATA_PATH"):
    settings.DATA_PATH = pathlib.Path(settings.DATA_PATH)
else:
    settings.DATA_PATH = (
        pathlib.Path(data_pipeline.__file__).resolve().parent.parent.parent
        / "ejpipeline_data"
    )

if type(settings.DATA_PATH) is list:
    settings.DATA_PATH = settings.DATA_PATH[0]

print(settings.DATA_PATH)
settings.DATA_PATH.mkdir(parents=True, exist_ok=True)

# `envvar_prefix` = export envvars with `export EDGI_DATA_PATH=bar`.
# `settings_files` = Load these files in the order.
