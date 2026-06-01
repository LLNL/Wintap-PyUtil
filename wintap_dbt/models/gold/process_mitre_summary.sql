select
    cast(null as varchar) pid_hash,
    cast(null as {{ string_array_type() }}) mitre_analytic_ids,
    cast(null as {{ string_array_type() }}) mitre_information_domains,
    cast(null as {{ string_array_type() }}) mitre_subtypes,
    cast(null as {{ string_array_type() }}) mitre_analytic_types,
    cast(0 as bigint) mitre_num_rows
where false
