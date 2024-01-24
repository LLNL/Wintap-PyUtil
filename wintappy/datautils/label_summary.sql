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
