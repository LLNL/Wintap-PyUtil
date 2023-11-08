import argparse
import logging
import sys
from importlib.resources import files as resource_files

from wintappy.config import get_config, print_config
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging, daterange, get_date_range


def process_range(cur_dataset, start_date, end_date):
    for single_date in daterange(start_date, end_date):
        daypk = single_date.strftime("%Y%m%d")
        con = ru.init_db()
        globs = ru.get_globs_for(cur_dataset, daypk)
        # No need to pass dayPK as the globs already include it.
        ru.create_raw_views(con, globs)
        ru.run_sql_no_args(
            con, resource_files("wintappy.datautils").joinpath("rawtostdview.sql")
        )
        ru.write_parquet(
            con, cur_dataset, ru.get_db_objects(con, exclude=["tmp"]), daypk
        )
        con.close()


def main(argv=None) -> None:
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="rawtorolling.py",
        description="Convert raw Wintap data into standard form, partitioned by day",
    )
    parser.add_argument("-d", "--dataset", help="Path to the dataset dir to process")
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")
    parser.add_argument("-c", "--config", help="Path to config file")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
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

    print_config(args)

    start_date, end_date = get_date_range(args.START, args.END)

    process_range(args.DATASET, start_date, end_date)


if __name__ == "__main__":
    main(argv=None)
