-- LOLBAS source: https://github.com/LOLBAS-Project/LOLBAS
-- Join PROCESS to LOLBAS using the filename. Produces many false positives.
-- Future: figure out how to include arguments in the join to reduce false positives.

-- Summarize LOLBAS hits to PID_HASH
create table process_lolbas_summary
as
select pls.pid_hash, pls.lolbas_privs, pls.lolbas_cats, pls.lolbas_mitre, lr.lolc_class, count(*) lolbas_num_rows,
from (
	select pid_hash,
      p.process_name,
      p.args,
	  list_sort(list(distinct Command_Privileges)) lolbas_privs,
	  list_sort(list(distinct Command_Category)) lolbas_cats,
	  list_sort(list(distinct MITRE_ATTCK_technique)) lolbas_mitre
	from main.process p
	join lolbas l on lower(l.filename)=p.process_name
	group by all
) pls
left outer join lolc_labels lr on pls.process_name=lr.process_name and pls.args=lr.args
group by all
;
