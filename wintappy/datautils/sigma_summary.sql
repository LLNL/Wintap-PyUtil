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

-- Create a summary view for all NetworkX (Everest) labels
create or replace view labels_graph_nodes
as
select
	filename,
	node->>'type' node_type,
	node->>'id' id,
	node->>'annotation' annotation,
	case
		when node_type = 'Host' then node->>'$.HostName[0].id'
		when node_type = 'Process' then node->>'$.ProcessDetails[0].ProcessName'
		when node_type = 'File' then node->>'$.FileKey[0].Filename'
		when node_type = 'FiveTupleConn' then concat_ws(':',node->>'$.FiveTupleKey[0].protocol',node->>'$.FiveTupleKey[0].RemoteIp', node->>'$.FiveTupleKey[0].RemotePort')
		when node_type = 'IpConn' then node->>'$.IpConnKey[0].ID'
		else node_type||' missing in view'
	end as label
from
	(
	select
		unnest(nodes) as node,
		filename
	from
		labels_networkx)
order by
	all
;

-- Process node graph labels summarized by PID_HASH
create or replace view labels_graph_process_summary
as
select id pid_hash, 'networkx' as label_source, count(distinct filename) label_num_sources, count(distinct annotation) label_num_uniq_annotations, count(*) label_num_hits
from labels_graph_nodes 
where node_type ='Process'
group by ALL 
;

-- Network node graph labels summarized by CONN_ID
-- Note: This view is just created for convenience for users later and must be joined to base network data.
create or replace view labels_graph_net_conn
as
select id conn_id, 'networkx' as label_source, count(distinct filename) label_num_sources, count(distinct annotation) label_num_uniq_annotations, count(*) label_num_hits
from labels_graph_nodes 
where node_type ='FiveTupleConn'
group by ALL 
;

-- Recreate PROCESS_SUMMARY with SIGMA, NetworkX labels added. Create a single column for totals to simplify filtering.
create or replace view process_summary_labels
as
select ps.*,
  sl.* exclude (pid_hash),
  ifnull(sl.critical_num_sigma_hits,0)+ifnull(sl.high_num_sigma_hits,0)+ifnull(sl.medium_num_sigma_hits,0)+ifnull(sl.low_num_sigma_hits,0) total_sigma_hits,
  gps.* exclude (pid_hash),
from process_summary ps
left outer join sigma_labels_summary sl on ps.pid_hash=sl.pid_hash
left outer join labels_graph_process_summary gps on ps.pid_hash=gps.pid_hash 
;


-- select label_source, total_sigma_hits, count(*) from main.process_summary_labels group by all order by all 
