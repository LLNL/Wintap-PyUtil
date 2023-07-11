import argparse
import logging
import sys
from datetime import datetime

from wintappy.datautils import rawutil as ru


def main():
    parser = argparse.ArgumentParser(
        prog="rawtostdview.py",
        description="Convert raw Wintap data into standard form, no partitioning",
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

    con = ru.init_db()
    globs = ru.get_glob_paths_for_dataset(cur_dataset, subdir="rolling", include="raw_")
    ru.create_raw_views(con, globs, args.start, args.end)
    ru.run_sql_no_args(con, "./rawtostdview.sql")
    ru.write_parquet(con, cur_dataset, ru.get_db_objects(con, exclude=["raw_", "tmp"]))


if __name__ == "__main__":
    main()
