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
    parser.add_argument(
        "--portable",
        help="Copy data from source parquet files into the duckdb. Resulting db file is portable.",
        action="store_true",
    )
    env_config = EnvironmentConfig(parser)
    env_config.add_aggregation_level(required=True)
    env_config.add_dataset_path(required=True)
    args = env_config.get_options(argv)

    fqds = os.path.abspath(args.DATASET)

    # Set path and name for helperdb file.
    dbname = args.NAME if "NAME" in args else args.AGGLEVEL
    dbpath = f"{args.PATH}" if "PATH" in args else f"{fqds}{os.sep}dbhelpers"
    portable = args.PORTABLE if "PORTABLE" in args else False
    if not os.path.exists(dbpath):
        os.makedirs(dbpath)
        logging.debug(f"created folder: {dbpath} ")

    logging.info(f"Writing helperdb to: {dbpath}{os.sep}{dbname}")
    # Always start with rolling
    logging.info("\n  Creating rolling views...\n")

    # Fix lookups! very fragile here...
    helperdb = ru.init_db(
        fqds,
        agg_level="rolling",
        database=f"{dbpath}{os.sep}{dbname}.db",
        lookups=f"{fqds}/../lookups",
    )
    # Layer in the requested agglevel if it ISN'T rolling
    if args.AGGLEVEL.lower() != "rolling":
        # Create everything in stdview-Start-End, this will replace any views defined in rolling that got recreated, such as HOST, PROCESS, etc.
        logging.info(f"\n  Creating {args.AGGLEVEL} views...\n")
        globs = ru.get_glob_paths_for_dataset(fqds, subdir=args.AGGLEVEL)
        ru.create_views(helperdb, globs)
    helperdb.close()

    if portable and args.AGGLEVEL.lower() != "rolling":
        logging.info(
            f"\n  Creating portable version of database: {dbpath}{os.sep}portable-{dbname}\n"
        )
        # Create another DB that actually contains all the data, not pointers to the parquet. MUCH LARGER RESULT! But portable.
        # Currently, doesn't support copying rolling tables because they have no start/end filter and could be HUGE.
        # Ignores raw_ tables as there is no easy way to limit to a subset of dayPKs and the result could be way too big.

        # New database instance
        portabledbname = f"{dbpath}{os.sep}portable-{dbname}"
        # Using attach will create the db
        portabledb = ru.init_db(database=portabledbname + ".db")
        portabledb.sql(f"attach '{dbpath}{os.sep}{dbname}.db' as src (read_only true)")

        for table in ru.get_db_objects(portabledb, exclude=["raw_"]):
            logging.info(f"Copying {table}")
            portabledb.sql(f"create table {table} as select * from src.{table}")

        portabledb.close()


if __name__ == "__main__":
    main(argv=None)
