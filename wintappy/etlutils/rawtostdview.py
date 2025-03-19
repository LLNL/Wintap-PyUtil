import argparse
import logging
from importlib.resources import files as resource_files

from wintappy.config import EnvironmentConfig
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging


def hostnames(con):
    hosts = con.sql("select hostname from host order by all").fetchall()
    # List of tuples, with one element, so convert to a simple string list
    return [host[0] for host in hosts]


def init_process_path(con):
    ru.run_sql_no_args(
        con, resource_files("wintappy.datautils").joinpath("process_path_2.sql")
    )


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="rawtostdview.py",
        description="Convert raw Wintap data into standard form, no partitioning",
    )
    env_config = EnvironmentConfig(parser)
    env_config.add_start(required=False)
    env_config.add_end(required=False)
    env_config.add_dataset_path(required=True)
    env_config.add_aggregation_level(required=False)
    args = env_config.get_options(argv)

    # Note: default uses an memory database. For debugging, add 'database="debug.db"' for a file-based db in the current dir
    con = ru.init_db()
    globs = ru.get_glob_paths_for_dataset(
        args.DATASET, subdir="rolling", include="raw_"
    )
    logging.info(f"Processing ROLLING from {args.START} to {args.END}")
    ru.create_raw_views(con, globs, args.START, args.END)

    # Using a heuristic for process rows (what value? dunno?), when >, iterate on hostname to reduce the processing set.

    for sqlfile in ["rawtostdview.sql", "process_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))

    logging.info(f"Setting up for building Process_Path")
    init_process_path(con)
    hosts = hostnames(con)
    for hostname in hosts:
        logging.info(f"  Building Process_Path for: {hostname}")
        con.sql("drop table tmp_process")
        # Build a table with just one host in it.
        con.sql(f"create table tmp_process as from process where hostname='{hostname}'")
        con.sql("insert into process_path select * from process_path_v1")

    # Clean up
    con.sql("drop view process_path_v1")
    con.sql("drop table tmp_process")

    ru.write_parquet(
        con,
        args.DATASET,
        ru.get_db_objects(con, exclude=["raw_", "tmp"]),
        agg_level=f"stdview-{args.START}-{args.END}",
    )


if __name__ == "__main__":
    main(argv=None)
