import argparse
import logging
import sys, os

# from importlib.resources import files as resource_files
from pathlib import Path
from pprint import pprint

from wintappy.config import get_config
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="dbhelpers.py",
        description="Create DuckDB database instance configured with views to parquet. Simplifies client usage.",
    )
    parser.add_argument("-c", "--config", help="Path to config file")
    parser.add_argument("-d", "--dataset", help="Path to the dataset dir to process")
    parser.add_argument(
        "-a",
        "--agglevel",
        help="Aggregation level to map. This is one of the sub-directories of the dataset.",
        default="rolling",
    )
    parser.add_argument(
        "-p",
        "--path",
        help="Path for the duckdb created. Defaults to [dataset]/[dbhelper]/[agglevel].duckdb",
    )
    parser.add_argument(
        "-n",
        "--name",
        help="Name for the duckdb created. Defaults to [agglevel].duckdb",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        help="Logging Level: INFO, WARN, ERROR, DEBUG",
    )
    options, _ = parser.parse_known_args(argv)

    # setup config based on env variables and config file
    args = get_config(options.config)
    # update config with CLI args
    args.update({k: v for k, v in vars(options).items() if v is not None})

    try:
        logging.getLogger().setLevel(args.LOG_LEVEL)
    except ValueError:
        logging.error(f"Invalid log level: {args.LOG_LEVEL}")
        sys.exit(1)

    # Validate path for dataset and agglevel
    if Path(args.DATASET).is_dir():
        if Path(args.DATASET).joinpath(args.AGGLEVEL).is_dir():
            cur_dataset = args.DATASET
            cur_agglevel = args.AGGLEVEL
        else:
            logging.error(f"Invalid agglevel {args.AGGLEVEL}")
            print(f"Dataset: {args.DATASET} has agglevels:")
            for dir in os.listdir(args.DATASET):
                print(f"  {dir}")
            sys.exit(1)
    else:
        logging.error(f"Invalid dataset {args.DATASET}")
        sys.exit(1)

    # Set path and name for helperdb file.
    dbname = args.NAME if args.NAME else args.AGGLEVEL + ".db"
    dbfqfn = (
        f"{args.PATH}/{dbname}" if args.PATH else f"{cur_dataset}/dbhelpers/{dbname}"
    )

    print(f"Writing helperdb to: {dbfqfn}")
    # Always start with rolling
    print("\n  Creating rolling views...\n")

    # Fix lookups! very fragile here...
    helperdb = ru.init_db(cur_dataset, agg_level="rolling", database=dbfqfn, lookups=f'{cur_dataset}/../lookups')
    # Layer in the requested agglevel if it ISN'T rolling
    if cur_agglevel.lower() != "rolling":
        # Create everything in stdview-Start-End, this will replace any views defined in rolling that got recreated, such as HOST, PROCESS, etc.
        print(f"\n  Creating {cur_agglevel} views...\n")
        globs = ru.get_glob_paths_for_dataset(cur_dataset, subdir=cur_agglevel)
        ru.create_views(helperdb, globs)
    helperdb.close()


if __name__ == "__main__":
    main(argv=None)
