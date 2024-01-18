import argparse
import logging
import os
import sys
from pathlib import Path

from dynaconf import Dynaconf, ValidationError, Validator, inspect_settings, settings

WINTAP_CONFIG_FILE = "wintappy_settings.toml"


def _get_config_path(config_path: str = "") -> str:
    if config_path:
        return f"{config_path}{os.sep}{WINTAP_CONFIG_FILE}"
    return WINTAP_CONFIG_FILE


def _get_config(config_path: str = "") -> Dynaconf:
    # setup config params
    config = Dynaconf(
        envvar_prefix="WINTAPPY",
        settings_files=[_get_config_path(config_path)],
        # Note, setting default here "" results in None after processing.
        validators=[
            Validator("AWS_PROFILE", is_type_of=str, default=""),
            Validator("AWS_REGION", is_type_of=str, default=""),
            Validator("BUCKET", is_type_of=str),
            Validator("DATASET", is_type_of=str, default=os.getcwd()),
            Validator(
                "LOG_LEVEL",
                is_type_of=str,
                is_in=["INFO", "WARN", "ERROR", "DEBUG"],
                default="INFO",
            ),
            Validator("PREFIX", is_type_of=str),
            Validator("START", is_type_of=str, default=""),
            Validator("END", is_type_of=str, default=""),
            Validator("PATH", is_type_of=str, default=""),
            Validator("NAME", is_type_of=str, default=""),
            Validator("VIEW", is_type_of=str, default=""),
        ],
    )
    # validate the configs
    try:
        settings.validators.validate_all()
    except ValidationError as e:
        logging.error(e.details)
        raise e
    print_config(config)
    return config


def print_config(config: Dynaconf) -> None:
    # print out overall config
    logging.debug(f"config params: {inspect_settings(config)}")


def arg_parser(parser):
    """
    Define arguments for all CLIs
    """
    # Common arguments
    parser.add_argument(
        "-a",
        "--agglevel",
        help="Aggregation level to map. This is one of the sub-directories of the dataset.",
        default="rolling",
    )
    parser.add_argument("-c", "--config", help="Path to config file")
    parser.add_argument("-d", "--dataset", help="Path to the dataset dir to process")
    parser.add_argument(
        "-i", "--init", default=True, help="Initilize (create) directories"
    )
    parser.add_argument(
        "-l",
        "--log-level",
        help="Logging Level: INFO, WARN, ERROR, DEBUG",
    )
    return parser


def get_configs(parser, argv) -> Dynaconf:
    parser = arg_parser(parser)
    options, _ = parser.parse_known_args(argv)

    # setup config based on env variables and config file
    args = _get_config(options.config)
    # update config with CLI args
    args.update({k: v for k, v in vars(options).items() if v is not None})
    validate_args(args)
    return args


def validate_args(args):
    try:
        logging.getLogger().setLevel(args.LOG_LEVEL)
    except ValueError:
        logging.error(f"Invalid log level: {args.LOG_LEVEL}")
        # sys.exit(1)

    # Validate path for dataset and agglevel
    if Path(args.DATASET).is_dir():
        if not Path(args.DATASET).joinpath(args.AGGLEVEL).is_dir():
            logging.error(f"Invalid agglevel {args.AGGLEVEL}")
            print(f"Dataset: {args.DATASET} has agglevels:")
            for dir in os.listdir(args.DATASET):
                print(f"  {dir}")
            # sys.exit(1)
    else:
        logging.error(f"Invalid dataset {args.DATASET}")
        # sys.exit(1)
    return True
