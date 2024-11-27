#!/bin/bash
#
# Given a parquet source file, count rows. If additional args are present, assume they are columns to count distinct values for.

newline=$'\n'

target=$(basename "$1" .parquet)

source=$1

if [ -d "$1" ]; then
  source="$1/**/*.parquet"
fi

count_sql="select count(*) num_rows, count(distinct $2) num_$2 from '$source'"
~/apps/duckdb <<< "$count_sql"
