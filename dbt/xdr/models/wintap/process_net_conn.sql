{{ config(materialized='view') }}

SELECT *
from "{{ full_path('stdview-20240819-20240923', 'process_net_conn.parquet') }}"
