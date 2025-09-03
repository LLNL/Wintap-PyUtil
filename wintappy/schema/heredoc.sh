#!/bin/bash
target=$(basename "$1".parquet)
source=$1
if [ -d "$1" ]; then
    source="$1/**/*.parquet"
fi
~/apps/duckdb <<-SQL
select count(*) as num_rows, count(distinct $2) as num_$2
from '$source'
SQL