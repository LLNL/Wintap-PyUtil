#!/bin/bash
#
# Generate view SQL for the given directory of parquet.
# Note: assumes passed dir has one or more parquet files or hive dirs of parquet.

newline=$'\n'

for entity in $1/*;
do
    source=""
    if [ -d "$entity" ]; then
      source="$entity/**/*.parquet"
      sql_name=$entity
    else
      source=$entity
      sql_name=$(basename "$entity" .parquet)
    fi
    echo "create view $sql_name as from '$source';"
done
