#!/bin/bash
viewname=$(basename "$1" ".parquet")
source=$1
if [ -d "$1" ]; then
    source="$1/**/*.parquet"
fi
cmd="create view $viewname as from '$source'"
echo $cmd
~/apps/duckdb -cmd "$cmd; describe $viewname;"
