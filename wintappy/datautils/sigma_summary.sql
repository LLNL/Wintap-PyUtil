-- Create a table summarizing all SIGMA hits by pid_hash.
-- Note: this fails as a view due to the PIVOT
create table sigma_labels_summary
as
pivot 
(select
	entity pid_hash,
	s.level sigma_level,
	count(*) num_rows,
	count(distinct daypk) num_daypk,
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
using first(num_analytic_ids) num_sigma_hits, first(num_daypk) num_sigma_daypk, first(num_rows) num_sigma_rows
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


-- Recreate PROCESS_SUMMARY with SIGMA, NetworkX labels added. Create a single column for totals to simplify filtering.
create or replace view process_summary_sigma
as
select ps.*,
  sl.* exclude (pid_hash),
  ifnull(sl.critical_num_sigma_hits,0)+ifnull(sl.high_num_sigma_hits,0)+ifnull(sl.medium_num_sigma_hits,0)+ifnull(sl.low_num_sigma_hits,0) total_sigma_hits,
from process_summary ps
left outer join sigma_labels_summary sl on ps.pid_hash=sl.pid_hash
;
