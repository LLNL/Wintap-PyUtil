-- Create a table summarizing all SIGMA hits by pid_hash.
-- Note: this fails as a view due to the PIVOT
create table sigma_labels_summary
as
pivot 
(select
	entity pid_hash,
	s.level sigma_level,
	count(*) num_rows,
	count(distinct sl.analytic_id) num_analytic_ids
from
	sigma_labels sl
left outer join sigma s on
	sl.analytic_id = s.id
where sl.entity_type ='pid_hash'
group by
	all
)
on sigma_level
using first(num_analytic_ids) num_sigma_hits, first(num_rows) num_sigma_rows
;

-- The PIVOT statement will dynamically create columns from the values in the LEVEL column. 
-- To ensure the final schema has all expected columns, add them explicitly here if missing.
alter table sigma_labels_summary add column if not exists critical_num_sigma_hits numeric
;
alter table sigma_labels_summary add column if not exists high_num_sigma_hits numeric
;
alter table sigma_labels_summary add column if not exists medium_num_sigma_hits numeric
;
alter table sigma_labels_summary add column if not exists low_num_sigma_hits numeric
;
