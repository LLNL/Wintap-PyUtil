-- Create PROCESS_SUMMARY

-- Create summaries for detail event types
create or replace view process_registry_summary
--# required
--# template: stdview
as
SELECT
  agent_id,
  hostname,
  pid_hash,
  process_name,
  sum(CASE WHEN activity_type = 'READ' THEN event_count ELSE 0 END) reads,
  sum(CASE WHEN activity_type = 'WRITE' THEN event_count ELSE 0 END) writes,
  sum(CASE WHEN activity_type = 'CREATEKEY' THEN event_count ELSE 0 END) createkeys,
  sum(CASE WHEN activity_type = 'DELETEKEY' THEN event_count ELSE 0 END) deletekeys,
  sum(CASE WHEN activity_type = 'DELETEVALUE' THEN event_count ELSE 0 END) deletevalues,
  min(first_seen) first_seen,
  --max(last_seen) last seen,
  sum(event_count) total_activity_types
FROM process_registry
GROUP BY ALL
;
 
create or replace view process_file_summary
--# required
--# template: stdview
as
SELECT
  agent_id,
  hostname,
  process_name,
  pid_hash,
  -- Note: Delete is always 0 bytes, so, don't create a column for it.
  sum(CASE WHEN activity_type = 'CLOSE' THEN event_count ELSE 0 END) Close_Events,
  sum(CASE WHEN activity_type = 'CREATE' THEN event_count ELSE 0 END) Create_Events,
  sum(CASE WHEN activity_type = 'DELETE' THEN event_count ELSE 0 END) Delete_Events,
  sum(CASE WHEN activity_type = 'RENAME' THEN event_count ELSE 0 END) Rename_Events,
  sum(CASE WHEN activity_type = 'SETINFO' THEN event_count ELSE 0 END) SetInfo_Events,
  sum(CASE WHEN activity_type = 'READ' THEN bytes_Requested ELSE 0 END) Read_Bytes,
  sum(CASE WHEN activity_type = 'READ' THEN event_count ELSE 0 END) Read_Events,
  sum(CASE WHEN activity_type = 'WRITE' THEN bytes_Requested ELSE 0 END) Write_Bytes,
  sum(CASE WHEN activity_type = 'WRITE' THEN event_count ELSE 0 END) Write_Events,
  sum(num_raw_rows) num_raw_rows,
  count(DISTINCT file_hash) num_uniq_file_hash,
  sum(CASE WHEN filename IS NULL THEN 1 ELSE 0 END) num_null_filename,
  min(first_seen) first_seen,
  max(last_seen) last_seen
FROM process_file
GROUP BY all
;

CREATE OR REPLACE VIEW process_net_summary
--# required
--# template: stdview
AS
SELECT
	os_family,
	pid_hash,
	process_name,
    agent_id,
	hostname,
	count(DISTINCT conn_id) conn_id_count,
	sum(total_events) net_total_events,
	sum(total_size) net_total_size,
	sum(num_raw_rows) num_raw_rows,
	sum(tcp_accept_count) tcp_accept_count,
	sum(tcp_connect_count) tcp_connect_count,
	sum(tcp_disconnect_count) tcp_disconnect_count,
	sum(tcp_reconnect_count) tcp_reconnect_count,
	sum(tcp_recv_count) tcp_recv_count,
	sum(tcp_recv_size) tcp_recv_size,
	sum(tcp_retransmit_count) tcp_retransmit_count,
	sum(tcp_send_count) tcp_send_count,
	sum(tcp_send_size) tcp_send_size,
	sum(tcp_tcpcopy_count) tcp_tcpcopy_count,
	sum(tcp_tcpcopy_size) tcp_tcpcopy_size,
	sum(udp_recv_count) udp_recv_count,
	sum(udp_recv_size) udp_recv_size,
	sum(udp_send_count) udp_send_count,
	sum(udp_send_size) udp_send_size,
	min(first_seen) first_seen,
	max(last_seen) last_seen,
	-- Communication Metrics TCP/UDP. Do we need packet counts also?
	sum(ifnull(tcp_recv_size, 0)+ ifnull(udp_recv_size, 0)) net_recv_size,
	sum(ifnull(tcp_send_size, 0)+ ifnull(udp_send_size, 0)) net_send_size,
	sum((ifnull(tcp_recv_size, 0)+ ifnull(udp_recv_size, 0))+(ifnull(tcp_send_size, 0)+ ifnull(udp_send_size, 0))) net_rs_total,
	(sum(ifnull(tcp_send_size, 0)+ ifnull(udp_send_size, 0))/ sum((ifnull(tcp_recv_size, 0)+ ifnull(udp_recv_size, 0))+ (ifnull(tcp_send_size, 0)+ ifnull(udp_send_size, 0)))) net_send_vs_recv,
	-- Communication Metrics TCP
	-- Do we need tcp_total_size? Need to check other TCP event numbers and see if it really makes sense.
	sum(ifnull(tcp_recv_size, 0)+ ifnull(tcp_send_size, 0)) tcp_rs_total,
	(sum(ifnull(tcp_send_size, 0))/ sum((ifnull(tcp_recv_size, 0))+(ifnull(tcp_send_size, 0)))) tcp_send_vs_recv,
	-- Communication Metrics UDP
	-- For UDP, we definitely don't need both udp_total_size and udp_rs_total as there are only SEND/RECV types.
	sum(ifnull(udp_recv_size, 0)+ ifnull(udp_send_size, 0)) udp_rs_total,
	(sum(ifnull(udp_send_size, 0))/ sum((ifnull(udp_recv_size, 0))+(ifnull(udp_send_size, 0)))) udp_send_vs_recv,
	-- Summary Statistics
	min(total_size) min_bytes,
	max(total_size) max_bytes,
	avg(total_size) avg_bytes,
	min(total_events) min_packets,
	max(total_events) max_packets,
	avg(total_events) avg_packets,
	sum(sq_size) sq_size
FROM
	process_net_conn
GROUP BY
	ALL
;

CREATE OR REPLACE VIEW process_image_load_summary
--# required
--# template: stdview
AS
SELECT 
  agent_id,
  hostname,
  pid_hash,
  process_name,
  list_sort(list(distinct filename)) dlls,
  len(dlls) num_uniq_files,
--  max(num_uniq_build_times) max_uniq_build_times,
--  max(num_uniq_Checksums) max_uniq_checksums,
--  max(num_image_size) max_uniq_image_size,
  min(first_seen) first_seen,
  max(last_seen) last_seen
FROM PROCESS_IMAGE_LOAD
GROUP BY ALL
;


-- Create the PROCESS_SUMMARY
create or replace view process_summary
as
SELECT
	-- Process
	p.*,
	-- TODO: There is a chance the resulting INTERVAL is negative, which is not supported in parquet. So, convert to a double.
	 millisecond(p.PROCESS_TERM-p.PROCESS_STARTED)/1000 duration_seconds,
	-- Registry
	r.total_activity_types reg_totals,
	r.reads reg_reads,
	r.writes reg_writes,
	r.createkeys reg_createkeys,
	r.deletekeys reg_deletekeys,
	r.deletevalues reg_deletevalues,
	-- File
	f.Close_Events,
	f.Create_Events,
	f.Delete_Events,
	f.Rename_Events,
	f.SetInfo_Events,
	f.Read_Bytes,
	f.Read_Events,
	f.Write_Bytes,
	f.Write_Events,
	f.num_raw_rows file_num_raw_rows,
	f.num_uniq_file_hash,
	f.num_null_filename,
	f.first_seen file_first_seen,
	f.last_seen file_last_seen,
	-- Network
	n.conn_id_count,
	n.net_total_events,
	n.net_total_size,
	n.num_raw_rows net_num_raw_rows,
	n.tcp_accept_count,
	n.tcp_connect_count,
	n.tcp_disconnect_count,
	n.tcp_reconnect_count,
	n.tcp_recv_count,
	n.tcp_recv_size,
	n.tcp_retransmit_count,
	n.tcp_send_count,
	n.tcp_send_size,
	n.tcp_tcpcopy_count,
	n.tcp_tcpcopy_size,
	n.udp_recv_count,
	n.udp_recv_size,
	n.udp_send_count,
	n.udp_send_size,
	n.net_recv_size,
	n.net_send_size,
	n.net_rs_total,
	n.net_send_vs_recv,
	n.tcp_rs_total,
	n.tcp_send_vs_recv,
	n.udp_rs_total,
	n.udp_send_vs_recv,
	n.min_bytes,
	n.max_bytes,
	n.avg_bytes,
	n.min_packets,
	n.max_packets,
	n.avg_packets,
	n.sq_size,
	n.first_seen net_first_seen,
	n.last_seen net_last_seen,
	-- Image Loads
	i.dlls,
	i.num_uniq_files dll_num_uniq_files,
--	i.max_uniq_build_times dll_max_uniq_build_times,
--	i.max_uniq_checksums dll_max_uniq_checksums,
--	i.max_uniq_image_size dll_max_uniq_image_size,
	i.first_seen dll_first_seen,
	i.last_seen dll_last_seen,
	-- Host
	h.OS,
	h.OS_VERSION,
	h.ARCH
FROM
	PROCESS p
JOIN HOST h ON
	h.HOSTNAME = p.HOSTNAME
LEFT OUTER JOIN process_REGISTRY_SUMMARY r ON
	(p.PID_HASH = r.PID_HASH)
LEFT OUTER JOIN PROCESS_NET_SUMMARY n ON
	(p.PID_HASH = n.PID_HASH)
LEFT OUTER JOIN process_file_summary f ON
	(p.pid_hash = f.pid_hash)
LEFT OUTER JOIN PROCESS_IMAGE_LOAD_SUMMARY i ON
	(p.pid_hash = i.pid_hash)
;
