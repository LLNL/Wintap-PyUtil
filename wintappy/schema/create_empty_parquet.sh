#!/bin/bash
#
# Given a parquet source file, create a new, empty one from it. The intent is to have a parquet file with just the schema.

target=$(basename "$1")
copy_cmd="COPY (from '$1' where false) TO '$target' (FORMAT 'parquet');"

~/apps/duckdb -s "$copy_cmd"