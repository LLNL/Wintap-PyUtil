select
    cast(null as varchar) pid_hash,
    cast(null as {{ string_array_type() }}) lolbas_privs,
    cast(null as {{ string_array_type() }}) lolbas_cats,
    cast(null as {{ string_array_type() }}) lolbas_mitre,
    cast(null as varchar) lolc_class,
    cast(0 as bigint) lolbas_num_rows
where false
