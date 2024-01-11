import argparse
import logging
from importlib.resources import files as resource_files

from wintappy.config import get_configs
from wintappy.datautils import rawutil as ru
from wintappy.datautils import summary_util as su
from wintappy.etlutils.utils import configure_basic_logging


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="label_summary.py",
        description="Convert networkx JSON to table summarized by PID_HASH",
    )
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")

    args = get_configs(parser, argv)

    con = ru.init_db()

    # Create a views for the NETWORKX and PROCESS table. Evolve this into creating views for "dependent" table types.
    su.create_networkx_view(con, args.DATASET)
    su.create_process_view(con, args.DATASET, args.AGGLEVEL)
    logging.debug(con.execute("show tables").fetchall())
    for sqlfile in ["label_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
    logging.debug(con.execute("show tables").fetchall())
    ru.write_parquet(
        con,
        args.DATASET,
        ru.get_db_objects(con, exclude=["raw_", "tmp"]),
        agg_level=f"{args.AGGLEVEL}",
    )


if __name__ == "__main__":
    main(argv=None)
