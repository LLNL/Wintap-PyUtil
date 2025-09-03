#!/bin/bash
#
# Given a parquet source file, generate the SQL DDL for it

newline=$'\n'

target=$(basename "$1" .parquet)
create_table="create table $target as (from '$1' where false);$newline"
gen_schema=".schema $target --indent"
~/apps/duckdb <<< "$create_table$gen_schema"
