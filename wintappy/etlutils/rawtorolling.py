import argparse
import logging
from importlib.resources import files as resource_files
from typing import List, Dict

from wintappy.config import EnvironmentConfig
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging, daterange, get_date_range


def process_range(cur_dataset: str, start_date, end_date, exclude_event_types: List[str]) -> Dict:
    for single_date in daterange(start_date, end_date):
        daypk = single_date.strftime("%Y%m%d")
        con = ru.init_db()
        globs = ru.get_globs_for(cur_dataset, daypk)
        # No need to pass dayPK as the globs already include it.
        # TODO Skip processing of raw_memorymap to save some time...
        for skip_type in exclude_event_types:
            try:
                logging.info(f"Skipping path: {globs.get(skip_type)}")
                globs.pop(skip_type)
            except KeyError:
                pass
        ru.create_raw_views(con, globs)
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
    parser.add_argument('--exclude_event_types', type=EnvironmentConfig.list_of_strings, default=[])
    env_config = EnvironmentConfig(parser)
    env_config.add_start(required=False)
    env_config.add_end(required=False)
    env_config.add_dataset_path(required=True)
    args = env_config.get_options(argv)

    start_date, end_date = get_date_range(
        args.START, args.END, data_set_path=args.DATASET
    )

    logging.info(f"Processing {start_date} to {end_date}")
    process_range(args.DATASET, start_date, end_date, exclude_event_types=args.exclude_event_types)


if __name__ == "__main__":
    main(argv=None)
