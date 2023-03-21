import argparse
from datetime import datetime
import logging
import os
import platform
import sys
import hashlib
import tarfile

import duckdb
import pandas as pd
import rawutil as ru

def get_data(datapath):
    '''
    Get the unique_process_df panda df
    '''
    if os.path.isfile(datapath):
        return pd.read_parquet(datapath),  os.path.dirname(datapath)
    else:
        return get_data_from_stdview(datapath), f'{args.dataset}/binaries'

def get_data_from_stdview(datapath):
    stmt = f'''
    create or replace view process as select * from '{datapath}/process.parquet'
    '''
    con = ru.initdb()
    cursor = con.cursor()
    cursor.execute(stmt)

    stmt = '''
    SELECT process_name, process_path, file_md5, count(*) num_procs
    FROM PROCESS
    WHERE file_md5 IS NOT NULL AND file_md5<>'NA'
    GROUP BY 1,2,3
    ORDER BY 4
    '''
    unique_process_df = cursor.execute(stmt).fetchdf()
    cursor.close()
    con.close()
    return unique_process_df

def calcHash(filename):
    '''
    Calculate the MD5 for a file
    '''
    md5_hash = hashlib.md5()
    with open(filename,"rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096),b""):
            md5_hash.update(byte_block)        
        return str(md5_hash.hexdigest()).upper()

def main():
    parser = argparse.ArgumentParser( prog='collectbinaries.py', description='Collect all binaries found with matching md5 hashes and paths from a Wintap Process collect.')
    parser.add_argument('-d','--dataset', help='Path to the dataset dir to use as a source. The stdview/process table will be used.')
    parser.add_argument('-s','--summary', default=False, help='Print the summary of what is found. Does not write the tar file of binaries')
    parser.add_argument('-l', '--log-level', default='INFO', help='Logging Level: INFO, WARN, ERROR, DEBUG')
    args = parser.parse_args()
    
    try:
        logging.basicConfig(level=args.log_level,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    except ValueError:
        logging.error('Invalid log level: {}'.format(args.log_level))
        sys.exit(1)

    
    (unique_process_df, output_path) = get_data(args.dataset)
    # Exhaustive search for all files by path, then comparing MD5s
    # Results are stored as new columns in the panda
    for index, row in unique_process_df.iterrows():
        curPath=row['process_path']
        unique_process_df.at[index,'bin_found']=False
        unique_process_df.at[index,'md5_match']=False
        if(os.path.isfile(f'{curPath}')):
            unique_process_df.at[index,'bin_found']=True
            md5hash=calcHash(f'{curPath}')
            if(md5hash==row['file_md5']):
                unique_process_df.at[index,'md5_match']=True
                logging.info(f"Found:  {curPath}")


    
    if not os.path.exists(output_path):
        os.makedirs(output_path)
        logging.debug("folder '{}' created ".format(output_path))

    collect_ts = datetime.strftime(datetime.now(), "%Y%m%d_%H_%M")
    pmdfile=os.path.join(output_path,f'process_collect_{platform.node()}_{collect_ts}.parquet')
    logging.info(f'Writing process binaries metadata to {pmdfile}')
    # Write to a parquet file. This will be inlcuded with the binaries.
    unique_process_df.to_parquet(pmdfile)

    if not args.summary:
        # Collect binaries into a tar file
        bintgz=os.path.join(output_path,f'binaries_{platform.node()}_{collect_ts}.tgz')
        logging.info(f'Writing process binaries to {bintgz}')
        tar = tarfile.open(bintgz, 'w:gz')
        for index, row in unique_process_df.loc[unique_process_df.md5_match].iterrows():
            curPath=row['process_path']
            tar.add(f'{curPath}')
        tar.close()

    logging.info('Summary: ')
    print(duckdb.sql("select bin_found, md5_match, count(*) num_binaries from unique_process_df group by all order by all"))

if __name__ == '__main__':
    main()
