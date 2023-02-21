import argparse
import logging
import sys
from datetime import datetime, timedelta

from jinjasql import JinjaSql

import rawutil as ru


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def process_range(cur_dataset, start_date, end_date):
    jpy = JinjaSql()
    for single_date in daterange(start_date, end_date):
        daypk=single_date.strftime("%Y%m%d")
        con=ru.initdb()
        globs=ru.get_globs_for(cur_dataset,daypk)
        ru.create_raw_views(con,globs,jpy)
        ru.run_sql_no_args(con,'./RawToStdView.sql')
        ru.write_parquet(con,cur_dataset,daypk)
        con.close()
    
def main():
    parser = argparse.ArgumentParser( prog='rawtorolling.py', description='Convert raw Wintap data into standard form, partitioned by day')
    parser.add_argument('-d','--dataset', help='Path to the dataset dir to process')
    parser.add_argument('-s','--start', help='Start date (MM/DD/YYYY)')
    parser.add_argument('-e','--end', help='End date (MM/DD/YYYY)')
    parser.add_argument('-l', '--log-level', default='INFO', help='Logging Level: INFO, WARN, ERROR, DEBUG')
    args = parser.parse_args()
    
    try:
        logging.basicConfig(level=args.log_level,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    except ValueError:
        logging.error('Invalid log level: {}'.format(args.log_level))
        sys.exit(1)

    cur_dataset=args.dataset

    start_date=datetime.strptime(args.start, '%m/%d/%Y')
    end_date=datetime.strptime(args.end, '%m/%d/%Y')

    process_range(cur_dataset,start_date,end_date)

if __name__ == '__main__':
    main()
