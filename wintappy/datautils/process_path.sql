-- Considering now that the term "tree" is a misnomer: these are really "paths to root"
-- Had the concept of having a "tree_id", but what would that actually be? 

create table process_paths
as
select * from (
WITH RECURSIVE cte_process_tree (hostname, pid_hash, os_pid, process_name, process_path, LEVEL, parent_pid_hash, parent_os_pid, seq, ptree, daypk,ptree_list, ptree_list_tuples)
AS
   (select p.hostname,
          p.pid_hash,
          p.os_pid,
          p.process_name,
          p.process_path,
          0,
          p.parent_pid_hash,
          p.parent_os_pid,
          1,
          cast('='||p.process_name as varchar),
          p.daypk,
          [p.pid_hash],
          [{'pid_hash':p.pid_hash,'process_name':p.process_name}]
   from process p
   UNION ALL SELECT pt.hostname,
                    pt.pid_hash,
                    pt.os_pid,
                    pt.process_name,
                    pt.process_path,
                    pt.level + 1,
                    p.parent_pid_hash,
                    p.parent_os_pid,
                    pt.seq+1,
                    cast(pt.ptree||'->'||p.process_name as varchar),
                    p.daypk,
                    list_append(pt.ptree_list,p.pid_hash),
                    list_append(ptree_list_tuples,{'pid_hash':p.pid_hash,'process_name':p.process_name})
   FROM process p,
        cte_process_tree pt
   WHERE p.pid_hash = pt.parent_pid_hash
    AND pt.parent_pid_hash <> pt.pid_hash
     and not list_contains(pt.ptree_list,p.pid_hash)
     )
SELECT  max(level) over (partition by pid_hash) as max_level, *
FROM cte_process_tree
)
where max_level=level
;