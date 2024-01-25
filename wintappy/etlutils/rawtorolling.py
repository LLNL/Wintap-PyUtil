import argparse
from importlib.resources import files as resource_files

from wintappy.config import get_configs
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging, daterange, get_date_range


def process_range(cur_dataset, start_date, end_date):
    for single_date in daterange(start_date, end_date):
        daypk = single_date.strftime("%Y%m%d")
        con = ru.init_db()
        globs = ru.get_globs_for(cur_dataset, daypk)
        # No need to pass dayPK as the globs already include it.
        ru.create_raw_views(con, globs)
        # For now, processing REQUIREs that RAW_PROCESS_STOP exist even if its empty. Create an empty table if needed.
        ru.create_empty_process_stop(con)
        for sqlfile in ["rawtostdview.sql", "process_path.sql"]:
            ru.run_sql_no_args(
                con, resource_files("wintappy.datautils").joinpath(sqlfile)
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
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")

    args = get_configs(parser, argv)

    start_date, end_date = get_date_range(
        args.START, args.END, data_set_path=args.DATASET
    )

    logging.info(f"Processing {start_date} to {end_date}")
    process_range(args.DATASET, start_date, end_date)


if __name__ == "__main__":
    main(argv=None)
