import argparse
import logging
from importlib.resources import files as resource_files

from wintappy.config import get_configs
from wintappy.datautils import rawutil as ru
from wintappy.datautils import summary_util as su
from wintappy.etlutils.utils import configure_basic_logging

save_db_objects = []


def get_hosts(con):
    rows = con.execute("select distinct hostname from process order by all")
    return rows


def run_one(con, script):
    ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(script))


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="runone.py",
        description="Run one script over dataset",
    )
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")

    args = get_configs(parser, argv)

    # Use a persistent db may allow this to finish
    con = ru.init_db(database="faster-than-memory-maybe.db")
    # Create a view for the PROCESS table.
    logging.info(f"Creating PROCESS view: {args.DATASET} -> {args.AGGLEVEL}")
    su.create_process_view(con, args.DATASET, args.AGGLEVEL)

    logging.info(f"Creating Process Path")

    run_one(con, "process_path.sql")
    save_db_objects.extend(["process_path"])

    logging.debug(con.execute("show tables").fetchall())
    ru.write_parquet(
        con,
        args.DATASET,
        save_db_objects,
        agg_level=f"{args.AGGLEVEL}",
    )

    logging.info("Complete")


if __name__ == "__main__":
    main(argv=None)
