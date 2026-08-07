# Generate a summary for files provided

# Generate a single SQL statement so we get back all results together

import sys
import duckdb

def main(argv=None):
    sql='select "Filename":file_name, "Rows":format(\'{:t,}\',num_rows), "File Size":formatReadableDecimalSize(file_size_bytes::bigint) from ('
    union="\n"
    for i, filename in enumerate(sys.argv[1:]):
        sql+=union
        sql+=f"from parquet_file_metadata('{filename}')"
        union="\nunion\n"
    sql+=")\n"

    print(sql)

if __name__ == "__main__":
    main(argv=None)
    
# select "Filename":file_name, "Rows":format('{:t,}',num_rows), "File Size":formatReadableDecimalSize(file_size_bytes::bigint) from (from parquet_file_metadata('process.parquet') union from parquet_file_metadata('process_file.parquet') );