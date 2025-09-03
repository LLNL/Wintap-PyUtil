-- View for all nodes in the graph data
-- id is based on the node_type
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
		when node_type = 'IpV4Addr' then node->>'$.IpV4Addr[0].IP'
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

-- View for all links
-- To Do: map attribute features to sql specific SQL columns, which would make them easier to deal with later.
create or replace view labels_graph_links
as
select
		unnest(links, recursive:=true) as link,
		filename
	from
		labels_networkx
;

-- Macro for getting a simple name for the file
create macro tmp_basename(filename)
as
replace(string_split(filename,'/')[-1:][1],'.json','')
;

-- Process node graph labels summarized by PID_HASH
create or replace view labels_graph_process_summary
as
select id pid_hash, list_sort(list(distinct tmp_basename(filename))) as label_sources, count(distinct tmp_basename(filename)) label_num_sources, list_sort(list(distinct annotation)) label_annonations, count(distinct annotation) label_num_uniq_annotations, count(*) label_num_hits
from labels_graph_nodes 
where node_type ='Process'
group by ALL 
;

-- Network node graph labels summarized by CONN_ID
-- Note: This view is just created for convenience for users later and must be joined to base network data.
create or replace view labels_graph_net_conn
as
select id conn_id, list_sort(list(distinct tmp_basename(filename))) as label_sources, count(distinct tmp_basename(filename)) label_num_sources, list_sort(list(distinct annotation)) label_annonations, count(distinct annotation) label_num_uniq_annotations, count(*) label_num_hits
from labels_graph_nodes 
where node_type ='FiveTupleConn'
group by ALL 
;
