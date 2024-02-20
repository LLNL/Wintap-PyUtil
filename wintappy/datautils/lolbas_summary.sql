-- LOLBAS source: https://github.com/LOLBAS-Project/LOLBAS
-- Join PROCESS to LOLBAS using the filename. Produces many false positives.
-- Future: figure out how to include arguments in the join to reduce false positives.

-- Summarize LOLBAS hits to PID_HASH
create table process_lolbas_summary
as
select pid_hash, lolbas_privs, lolbas_cats, lolbas_mitre, count(*) lolbas_num_rows
from (
	select pid_hash,
	  list_sort(list(distinct Command_Privileges)) lolbas_privs,
	  list_sort(list(distinct Command_Category)) lolbas_cats,
	  list_sort(list(distinct MITRE_ATTCK_technique)) lolbas_mitre,
	from main.process p
	join lolbas l on lower(l.filename)=p.process_name 
	group by all
) group by all
;
