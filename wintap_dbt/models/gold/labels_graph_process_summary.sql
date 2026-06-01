select
    cast(null as varchar) pid_hash,
    cast(null as {{ string_array_type() }}) label_sources,
    cast(0 as bigint) label_num_sources,
    cast(null as {{ string_array_type() }}) label_annonations,
    cast(0 as bigint) label_num_uniq_annotations,
    cast(0 as bigint) label_num_hits
where false
