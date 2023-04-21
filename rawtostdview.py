import argparse
import logging
import sys

from jinjasql import JinjaSql

import rawutil as ru


def main():
    parser = argparse.ArgumentParser( prog='rawtostdview.py', description='Convert raw Wintap data into standard form, no partitioning')
    parser.add_argument('-d','--dataset', help='Path to the dataset dir to process')
    parser.add_argument('-l', '--log-level', default='INFO', help='Logging Level: INFO, WARN, ERROR, DEBUG')
    args = parser.parse_args()
    
    try:
        logging.basicConfig(level=args.log_level,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    except ValueError:
        logging.error('Invalid log level: {}'.format(args.log_level))
        sys.exit(1)

    cur_dataset=args.dataset
    jpy = JinjaSql()
    con = ru.initdb()
    globs=ru.get_glob_paths_for_dataset(cur_dataset)
    ru.create_raw_views(con,globs,jpy)
    ru.run_sql_no_args(con,'./RawToStdView.sql')
    ru.write_parquet(con,cur_dataset,ru.get_db_objects(con,exclude=['raw_','tmp']))

if __name__ == '__main__':
    main()

