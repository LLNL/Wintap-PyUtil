import argparse
import logging
import sys
from datetime import datetime, timedelta

from importlib_resources import files
from jinjasql import JinjaSql

from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import daterange

def process_range(cur_dataset, start_date, end_date):
    for single_date in daterange(start_date, end_date):
        daypk = single_date.strftime("%Y%m%d")
        con = ru.init_db()
        globs = ru.get_globs_for(cur_dataset, daypk)
        # No need to pass dayPK as the globs already include it.
        ru.create_raw_views(con, globs)
        ru.run_sql_no_args(
            con, files("wintappy.datautils").joinpath("rawtostdview.sql")
        )
        ru.write_parquet(
            con, cur_dataset, ru.get_db_objects(con, exclude=["tmp"]), daypk
        )
        con.close()


def main():
    parser = argparse.ArgumentParser(
        prog="rawtorolling.py",
        description="Convert raw Wintap data into standard form, partitioned by day",
    )
    parser.add_argument("-d", "--dataset", help="Path to the dataset dir to process")
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")
    parser.add_argument(
        "-l",
        "--log-level",
        default="INFO",
        help="Logging Level: INFO, WARN, ERROR, DEBUG",
    )
    args = parser.parse_args()

    try:
        logging.basicConfig(
            level=args.log_level,
            format="%(asctime)s %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S %p",
        )
    except ValueError:
        logging.error(f"Invalid log level: {args.log_level}")
        sys.exit(1)

    cur_dataset = args.dataset

    start_date = datetime.strptime(args.start, "%Y%m%d")
    end_date = datetime.strptime(args.end, "%Y%m%d")

    process_range(cur_dataset, start_date, end_date)


if __name__ == "__main__":
    main()
