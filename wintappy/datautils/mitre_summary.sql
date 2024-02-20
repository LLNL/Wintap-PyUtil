-- Summarize MITRE hits to PID_HASH

-- Note: Investigate prior mitre join layers to see where the info/substypes and analytic types are becoming lists.
create view process_mitre_summary
as
select
	ml.entity pid_hash,
	list_sort(list(distinct ml.analytic_id)) mitre_analytic_ids,
    list_sort(list(distinct mc.information_domain)) mitre_information_domains,
    list_sort(list(distinct mc.subtypes)) mitre_subtypes,
    list_sort(list(distinct mc.analytic_types)) mitre_analytic_types,
    count(*) mitre_num_rows
from 
    mitre_car mc
join mitre_labels ml ON ml.analytic_id = mc.id
group by all
;
