import argparse
import logging
import sys
from importlib.resources import files as resource_files

from wintappy.config import get_config
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging

def create_networkx_view(con, dataset):
    sql = f"create or replace view labels_networkx as select * from read_json_auto('{dataset}/labels/networkx/*.json', filename=true)"
    con.execute(sql)

def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="rawtostdview.py",
        description="Convert raw Wintap data into standard form, no partitioning",
    )
    parser.add_argument("-c", "--config", help="Path to config file")
    parser.add_argument("-d", "--dataset", help="Path to the dataset dir to process")
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")
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

    cur_dataset = args.DATASET

    con = ru.init_db()
    globs = ru.get_glob_paths_for_dataset(cur_dataset, subdir="rolling", include="raw_")
    ru.create_raw_views(con, globs, args.START, args.END)
    for sqlfile in ['rawtostdview.sql', 'process_summary.sql']:
        ru.run_sql_no_args(
            con, resource_files("wintappy.datautils").joinpath(sqlfile)
        )
    create_networkx_view(con,cur_dataset)
    ru.run_sql_no_args(
        con, resource_files("wintappy.datautils").joinpath("label_summary.sql")
    )
    ru.write_parquet(
        con,
        cur_dataset,
        ru.get_db_objects(con, exclude=["raw_", "tmp"]),
        agg_level=f"stdview-{args.START}-{args.END}",
    )


if __name__ == "__main__":
    main(argv=None)
