import argparse
import logging
import os

from wintappy.config import EnvironmentConfig
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="dbhelpers",
        description="Create DuckDB database instance configured with views to parquet. Simplifies client usage.",
    )

    # DBhelper
    # Should these be moved into its own function in config?
    parser.add_argument(
        "-n",
        "--name",
        help="Name for the duckdb created. Defaults to [agglevel].duckdb",
    )
    parser.add_argument(
        "-p",
        "--path",
        help="Path for the duckdb created. Defaults to [dataset]/[dbhelper]/[agglevel].duckdb",
    )
    env_config = EnvironmentConfig(parser)
    env_config.add_aggregation_level(required=True)
    env_config.add_dataset_path(required=True)
    args = env_config.get_options(argv)

    # Set path and name for helperdb file.
    dbname = args.NAME if args.NAME else args.AGGLEVEL + ".db"
    dbpath = f"{args.PATH}" if args.PATH else f"{args.DATASET}{os.sep}dbhelpers"
    if not os.path.exists(dbpath):
        os.makedirs(dbpath)
        logging.debug(f"created folder: {dbpath} ")

    logging.info(f"Writing helperdb to: {dbpath}{os.sep}{dbname}")
    # Always start with rolling
    logging.info("\n  Creating rolling views...\n")

    # Fix lookups! very fragile here...
    helperdb = ru.init_db(
        args.DATASET,
        agg_level="rolling",
        database=f"{dbpath}{os.sep}{dbname}",
        lookups=f"{args.DATASET}/../lookups",
    )
    # Layer in the requested agglevel if it ISN'T rolling
    if args.AGGLEVEL.lower() != "rolling":
        # Create everything in stdview-Start-End, this will replace any views defined in rolling that got recreated, such as HOST, PROCESS, etc.
        logging.info(f"\n  Creating {args.AGGLEVEL} views...\n")
        globs = ru.get_glob_paths_for_dataset(args.DATASET, subdir=args.AGGLEVEL)
        ru.create_views(helperdb, globs)
    helperdb.close()


if __name__ == "__main__":
    main(argv=None)
