-- pid_hash orphan checks for DBT-built Wintap/Lintap DuckDB databases.
--
-- Rule:
--   process.pid_hash is the primary key for process executions.
--   Every other model/table with pid_hash is a child of process.
--
-- Usage:
--   duckdb /path/to/wintap.duckdb < Wintap-PyUtil/wintap_dbt/qa/pid_hash_orphan_checks.sql
--
-- A healthy build should return orphan_rows = 0 for every check.

.mode duckbox

-- -----------------------------------------------------------------------------
-- Summary: one row per child relation.
-- -----------------------------------------------------------------------------

with orphan_checks as (
    select
        'process_conn_incr' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,
    from process_conn_incr c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_net_conn' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,
    from process_net_conn c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_file' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,
    from process_file c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_registry' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,
    from process_registry c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_image_load' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,
    from process_image_load c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_path' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,
    from process_path c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_registry_summary' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,
    from process_registry_summary c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_file_summary' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,

    from process_file_summary c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_net_summary' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,

    from process_net_summary c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_image_load_summary' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,

    from process_image_load_summary c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_summary' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,

    from process_summary c
    left join process p on c.pid_hash = p.pid_hash

    union all

    select
        'process_uber_summary' as child_relation,
        count(*) as child_rows,
        count(p.pid_hash) as matched_rows,
        count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
        count(*) filter (where c.pid_hash is null) as null_pid_hash_rows,
        count(distinct c.pid_hash) filter (where c.pid_hash is not null and p.pid_hash is null) as unique_orphan_pid_hash,

    from process_uber_summary c
    left join process p on c.pid_hash = p.pid_hash
)
select
    child_relation,
    child_rows,
    matched_rows,
    orphan_rows,
    unique_orphan_pid_hash,
    null_pid_hash_rows,
    case    
        when orphan_rows = 0 and null_pid_hash_rows = 0 then 'PASS'
        else 'FAIL'
    end as status
from orphan_checks
order by child_relation;

-- -----------------------------------------------------------------------------
-- Primary-key sanity check for process.pid_hash.
-- -----------------------------------------------------------------------------

select
    'process' as relation_name,
    count(*) as process_rows,
    count(pid_hash) as non_null_pid_hash_rows,
    count(*) filter (where pid_hash is null) as null_pid_hash_rows,
    count(distinct pid_hash) as distinct_pid_hash_rows,
    count(*) - count(distinct pid_hash) as duplicate_pid_hash_rows,
    case
        when count(*) filter (where pid_hash is null) = 0
         and count(*) = count(distinct pid_hash)
        then 'PASS'
        else 'FAIL'
    end as status
from process;

-- -----------------------------------------------------------------------------
-- Detail rows for investigation. Each query should return zero rows.
-- Keep limits low for quick terminal inspection.
-- -----------------------------------------------------------------------------

.print '\nOrphan process_conn_incr rows'
select c.pid_hash, c.process_name, count(*) unique_orphan_pids
from process_conn_incr c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
group by all
order by all
limit 25;

.print '\nOrphan process_net_conn rows'
select c.*
from process_net_conn c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
limit 25;

.print '\nOrphan process_file rows'
select c.*
from process_file c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
limit 25;

.print '\nOrphan process_registry rows'
select c.*
from process_registry c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
limit 25;

.print '\nOrphan process_image_load rows'
select c.*
from process_image_load c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
limit 25;

.print '\nOrphan process_path rows'
select c.*
from process_path c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
limit 25;

.print '\nOrphan process_summary rows'
select c.*
from process_summary c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
limit 25;

.print '\nOrphan process_uber_summary rows'
select c.*
from process_uber_summary c
left join process p on c.pid_hash = p.pid_hash
where c.pid_hash is not null and p.pid_hash is null
limit 25;

.print '\nDuplicate process.pid_hash values'
select
    pid_hash,
    count(*) as num_rows
from process
group by pid_hash
having count(*) > 1
order by num_rows desc, pid_hash
limit 25;
