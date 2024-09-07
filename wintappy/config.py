import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

from dynaconf import Dynaconf, ValidationError, Validator, inspect_settings

WINTAP_CONFIG_FILE = "wintappy_settings.toml"


class EnvironmentConfig:
    def __init__(self, parser: argparse.ArgumentParser):
        self.parser: argparse.ArgumentParser = parser
        self.dynaconf_validators: List[Validator] = [
            Validator(
                "LOG_LEVEL",
                is_type_of=str,
                is_in=["INFO", "WARN", "ERROR", "DEBUG"],
                default="INFO",
            ),
        ]
        # args that should be added to all
        self.parser.add_argument(
            "-l",
            "--log-level",
            help="Logging Level: INFO, WARN, ERROR, DEBUG",
            default="INFO",
        )

    def add_aggregation_level(self, required: bool = False) -> None:
        self.parser.add_argument(
            "-a",
            "--agglevel",
            help="Aggregation level to map. This is one of the sub-directories of the dataset.",
            default="rolling",
            required=required,
        )
        self.dynaconf_validators.append(
            Validator("AGGLEVEL", is_type_of=str, default="rolling")
        )

    def add_dataset_path(self, required: bool = False) -> None:
        self.parser.add_argument(
            "-d",
            "--dataset",
            help="Path to the dataset dir to process",
            required=required,
        )
        self.dynaconf_validators.append(Validator("DATASET", is_type_of=str))

    def add_start(
        self, required: bool = False, time_format: Optional[str] = "YYYYMMDD"
    ) -> None:
        self.parser.add_argument(
            "-s", "--start", help=f"Start date ({time_format})", required=required
        )
        self.dynaconf_validators.append(Validator("START", is_type_of=str, default=""))

    def add_end(
        self, required: bool = False, time_format: Optional[str] = "YYYYMMDD"
    ) -> None:
        self.parser.add_argument(
            "-e", "--end", help=f"End date ({time_format})", required=required
        )
        self.dynaconf_validators.append(Validator("END", is_type_of=str, default=""))

    def add_aws_settings(self, required: bool = False):
        # Note, setting default here "" results in None after processing.
        self.parser.add_argument("--aws-profile", help="AWS profile to use")
        self.parser.add_argument("-b", "--aws-s3-bucket", help="The S3 bucket")
        self.parser.add_argument(
            "-p", "--aws-s3-prefix", help="S3 prefix within the bucket"
        )
        self.dynaconf_validators.append(
            Validator("AWS_PROFILE", is_type_of=str, default="")
        )
        self.dynaconf_validators.append(
            Validator("AWS_REGION", is_type_of=str, default="")
        )
        self.dynaconf_validators.append(Validator("AWS_S3_BUCKET", is_type_of=str))
        self.dynaconf_validators.append(Validator("AWS_S3_PREFIX", is_type_of=str))

    def _get_config(self) -> Dynaconf:
        # setup config params
        config = Dynaconf(
            envvar_prefix="WINTAPPY",
            settings_files=[WINTAP_CONFIG_FILE],
            validators=self.dynaconf_validators,
        )
        # validate the configs
        try:
            config.validators.validate_all()
        except ValidationError as e:
            print(e.details)
            raise e
        return config

    def get_options(self, argv) -> Dynaconf:
        # setup config based on env variables and config file
        settings = self._get_config()
        # update required setting on argparser if set in the config file
        keys = settings.keys()
        for action in self.parser._actions:
            if action.dest.upper() in keys and settings[action.dest]:
                action.required = False
        # update config with CLI args
        options, _ = self.parser.parse_known_args(argv)
        settings.update({k: v for k, v in vars(options).items() if v is not None})
        self._validate_settings(settings)
        logging.debug(inspect_settings(settings))
        return settings

    def _validate_settings(self, args):
        try:
            logging.getLogger().setLevel(args.LOG_LEVEL)
        except ValueError:
            logging.error(f"Invalid log level: {args.LOG_LEVEL}. Defaulting to DEBUG")
            logging.getLogger().setLevel("DEBUG")

        # Validate path for dataset and agglevel
        if "DATASET" in args and Path(args.DATASET).is_dir():
            if (
                "AGGLEVEL" in args
                and not Path(args.DATASET).joinpath(args.AGGLEVEL).is_dir()
            ):
                logging.error(f"Invalid agglevel {args.AGGLEVEL}")
                print(f"Dataset: {args.DATASET} has agglevels:")
                for dir in os.listdir(args.DATASET):
                    print(f"  {dir}")

    # Support functions for argparser definitions.
    # Is there a better place to put these?
    def list_of_strings(self, arg: str)->List[str]:
        return arg.split(",")
