import argparse
import logging
from importlib.resources import files as resource_files

from wintappy.config import get_configs
from wintappy.datautils import rawutil as ru
from wintappy.datautils import summary_util as su
from wintappy.etlutils.utils import configure_basic_logging

save_db_objects = ["sigma_labels_summary", "process_summary_sigma"]


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
    if su.create_sigma_labels_view(con, args.DATASET, args.AGGLEVEL):
        save_db_objects.append("sigma_labels")
    su.create_process_view(con, args.DATASET, args.AGGLEVEL)
    su.create_sigma_lookups(con, args.DATASET)
    logging.debug(con.execute("show tables").fetchall())
    for sqlfile in ["sigma_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
    logging.debug(con.execute("show tables").fetchall())
    ru.write_parquet(
        con,
        args.DATASET,
        save_db_objects,
        agg_level=f"{args.AGGLEVEL}",
    )


if __name__ == "__main__":
    main(argv=None)
