import logging
import os

from dynaconf import Dynaconf, ValidationError, Validator, inspect_settings, settings

WINTAP_CONFIG_FILE = "wintappy_settings.toml"


def _get_config_path(config_path: str = "") -> str:
    if config_path:
        return f"{config_path}{os.sep}{WINTAP_CONFIG_FILE}"
    return WINTAP_CONFIG_FILE


def get_config(config_path: str = "") -> Dynaconf:
    # setup config params
    config = Dynaconf(
        envvar_prefix="WINTAPPY",
        settings_files=[_get_config_path(config_path)],
        validators=[
            Validator("AWS_PROFILE", is_type_of=str),
            Validator("AWS_REGION", is_type_of=str),
            Validator("BUCKET", is_type_of=str),
            Validator("DATASET", is_type_of=str, default=os.getcwd()),
            Validator("LOCAL_PATH", is_type_of=str, default=os.getcwd()),
            Validator(
                "LOG_LEVEL",
                is_type_of=str,
                is_in=["INFO", "WARN", "ERROR", "DEBUG"],
                default="INFO",
            ),
            Validator("PREFIX", is_type_of=str),
            Validator("START", is_type_of=str, default=""),
            Validator("END", is_type_of=str, default=""),
        ],
    )
    # validate the configs
    try:
        settings.validators.validate_all()
    except ValidationError as e:
        logging.error(e.details)
        raise e
    return config


def print_config(config: Dynaconf) -> None:
    # print out overall config
    logging.debug(f"config params: {inspect_settings(config)}")
