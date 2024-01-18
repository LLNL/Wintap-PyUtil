import argparse
from importlib.resources import files as resource_files

from wintappy.config import get_configs
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="rawtostdview.py",
        description="Convert raw Wintap data into standard form, no partitioning",
    )
    parser.add_argument("-s", "--start", help="Start date (YYYYMMDD)")
    parser.add_argument("-e", "--end", help="End date (YYYYMMDD)")

    args = get_configs(parser, argv)

    con = ru.init_db()
    globs = ru.get_glob_paths_for_dataset(
        args.DATASET, subdir="rolling", include="raw_"
    )

    ru.create_raw_views(con, globs, args.START, args.END)
    for sqlfile in ["rawtostdview.sql", "process_path.sql", "process_summary.sql"]:
        ru.run_sql_no_args(con, resource_files("wintappy.datautils").joinpath(sqlfile))
    ru.write_parquet(
        con,
        args.DATASET,
        ru.get_db_objects(con, exclude=["raw_", "tmp"]),
        agg_level=f"stdview-{args.START}-{args.END}",
    )


if __name__ == "__main__":
    main(argv=None)
