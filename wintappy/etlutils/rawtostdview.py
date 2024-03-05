import argparse
from importlib.resources import files as resource_files

from wintappy.config import EnvironmentConfig
from wintappy.datautils import rawutil as ru
from wintappy.etlutils.utils import configure_basic_logging


def main(argv=None):
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="rawtostdview.py",
        description="Convert raw Wintap data into standard form, no partitioning",
    )
    env_config = EnvironmentConfig(parser)
    env_config.add_start(required=False)
    env_config.add_end(required=False)
    env_config.add_dataset_path(required=True)
    env_config.add_aggregation_level(required=False)
    args = env_config.get_options(argv)

    # Note: default uses an memory database. For debugging, add 'database="debug.db"' for a file-based db in the current dir
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
