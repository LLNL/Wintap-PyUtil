import argparse
import logging
import os
import sys

import fsspec

from wintappy.config import EnvironmentConfig
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging

# These S3 helper functions should be moved into a new util package, eventually.
# def validate_s3():


def gen_duckdb_secret(con):
    # Note: DuckDB uses different key names than the cli.
    #  Also, ENDPOINT can't have the protocol!
    endpoint = os.environ["AWS_ENDPOINT_URL"].split("http://")[1]
    s3info = f"""
        CREATE OR REPLACE SECRET spk16s3 (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            URL_STYLE 'path',
            ENDPOINT '{endpoint}',
            USE_SSL '{os.environ['DUCKDB_USE_SSL']}',
            REGION '{os.environ['AWS_DEFAULT_REGION']}',
            KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}',
            SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}'
        )
    """
    print(s3info)
    con.sql(s3info)


def validate_s3():
    req_vars = set(
        [
            "DUCKDB_USE_SSL",
            "AWS_DEFAULT_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
        ]
    )
    # Validate that all env vars are set and work.
    if not req_vars.issubset(os.environ):
        logging.error(
            f"Missing S3 environment variables: {req_vars.difference(os.environ)}"
        )
        sys.exit()


def list_s3(s3path):
    fs = fsspec.filesystem("s3")
    tables = []
    for table in fs.ls(s3path):
        print(f"Processing: {table}")
        name = table.split("/")[-1:][0].split(".")[0]
        if fs.isdir(table):
            tables.append({"name": name, "path": f"s3://{table}/**/*.parquet"})
        else:
            tables.append({"name": name, "path": f"s3://{table}"})
    return tables


def create_views(con, s3path):
    gen_duckdb_secret(con)
    for table in list_s3(s3path):
        sql = f"create view {table['name']} as from '{table['path']}'"
        print(sql)
        con.sql(sql)
        print(con.sql(f"select count(*) from {table['name']}").fetchall())


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
        help="Copy data from source parquet files into the duckdb. Resulting db file is portable, although can be HUGE.",
        action="store_true",
    )
    parser.add_argument(
        "--S3",
        help="Create views using S3 paths. Requires S3 authentication tokens be available using AWS Boto3.",
        default="None",
    )
    env_config = EnvironmentConfig(parser)
    env_config.add_aggregation_level(required=True)
    env_config.add_dataset_path(required=True)
    args = env_config.get_options(argv)

    fqds = os.path.abspath(args.DATASET)

    # Set path and name for helperdb file.
    dbname = args.NAME if "NAME" in args else args.AGGLEVEL
    dbpath = f"{args.PATH}" if "PATH" in args else f"{fqds}{os.sep}dbhelpers"
    if not os.path.exists(dbpath):
        os.makedirs(dbpath)
        logging.debug(f"created folder: {dbpath} ")

    logging.info(f"Writing helperdb to: {dbpath}{os.sep}{dbname}")
    # Always start with rolling
    logging.info("\n  Creating rolling views...\n")

    # Fix lookups! very fragile here...
    helperdb = ru.init_db(
        dataset=None,
        agg_level="rolling",
        database=f"{dbpath}{os.sep}{dbname}.db",
    )
    # Layer in the requested agglevel if it ISN'T rolling
    if args.AGGLEVEL.lower() != "rolling":
        # Create everything in stdview-Start-End, this will replace any views defined in rolling that got recreated, such as HOST, PROCESS, etc.
        logging.info(f"\n  Creating {args.AGGLEVEL} views...\n")
        if args.S3 == "None":
            globs = ru.get_glob_paths_for_dataset(fqds, subdir=args.AGGLEVEL)
            ru.create_views(helperdb, globs)
        else:
            validate_s3()
            create_views(helperdb, f"{args.S3}/{args.AGGLEVEL}")
    helperdb.close()

    portable = args.PORTABLE if "PORTABLE" in args else False
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
