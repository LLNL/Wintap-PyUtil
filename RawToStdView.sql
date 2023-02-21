/* 
 * Create the standard view tables from existing RAW tables.
 * SQL Dialect: DuckDB
 *
 */

CREATE TABLE IF NOT EXISTS host
AS
SELECT
    -- Ignore host.id.hash for now as it is really only composed of the hostname.
    -- host.hostid.hash,
    hostname hostname,
    FIRST('windows') os_family,
    to_timestamp(min(eventtime)) first_seen,
    -- TODO: Should lastseen for host just be dropped? It isn't accurate. Better would be to derive it from other data: network, file, etc.
    to_timestamp(max(eventtime)) last_seen,
    first(os) os,
    count(DISTINCT os) num_os,
    first(CASE WHEN osVersion='' THEN NULL ELSE osVersion END) os_version,
    count(DISTINCT osVersion) num_os_version,
    first(CASE WHEN arch='' THEN NULL ELSE arch END) arch,
    count(DISTINCT arch) num_arch,
    first(processorCount) processor_count,
    count(DISTINCT processorCount) num_processor_count,
    first(processorSpeed) processor_speed,
    count(DISTINCT processorSpeed) num_processor_speed,
    first(hasBattery) has_battery,
    count(DISTINCT hasBattery) num_has_battery,
    first(DOMAIN) ad_domain,
    count(DISTINCT domain) num_ad_domain,
    first(domainRole) domain_role,
    count(DISTINCT domainRole) num_domain_role,
    max(lastBoot) last_boot,
    count(DISTINCT lastBoot) num_last_boot,
    max(WintapVersion) wintap_version,
    count(DISTINCT WintapVersion) num_wintap_version,
    max(ETLVersion) etl_version,
    count(DISTINCT ETLVersion) num_etl_version,
    max(Collectors) collectors,
    count(DISTINCT Collectors) num_collectors,
    count(*) num_rows
FROM raw_host
GROUP BY 1
;

CREATE TABLE IF NOT EXISTS host_ip
AS
SELECT
	hostname,
	FIRST('windows') os_family,
	CASE WHEN privateGateway='' THEN NULL ELSE privateGateway END private_gateway,
	int_to_ip(cast(ipaddr as bigint)) ip_addr_no,
	CASE WHEN mac='' THEN NULL ELSE mac END mac,
	ipaddr ip_addr,
	interface,
	mtu,
	to_timestamp(MIN(EventTime)) first_seen,
	to_timestamp(MAX(EventTime)) last_seen,
	COUNT(*) num_rows,
FROM RAW_MACIP
GROUP BY all
;

-- Summarize Process events into Process entities
CREATE TABLE IF NOT EXISTS process_tmp
AS
SELECT 
  p.pidhash pid_hash,
  -- osfamily will eventually come back as a partition key
  first('windows') os_family,
  first(p.Hostname) hostname,
  first(PID) os_pid,
  first(CASE WHEN p.processName='' THEN NULL ELSE p.processName END) process_name,
  count(DISTINCT p.processName) num_process_name,
  first(CASE WHEN p.ProcessArgs='' THEN NULL ELSE p.ProcessArgs END) args,
  count(DISTINCT p.ProcessArgs) num_args,
  -- Ignore useless names
  first(CASE WHEN p.userName='' OR lower(p.username)='na' THEN NULL ELSE p.username END) user_name,
  count(DISTINCT p.userName) num_user_name,
  first(CASE WHEN p.parentPidHash='' THEN NULL ELSE p.parentPidHash END) parent_pid_hash,
  count(DISTINCT p.parentPidHash) num_parent_pid_hash,
  first(p.parentPid) parent_os_pid,
  count(DISTINCT p.parentPid) num_parent_os_pid,
  first(CASE WHEN p.ProcessPath='' THEN NULL ELSE p.ProcessPath END) process_path,
  count(DISTINCT p.ProcessPath) num_process_path,
  -- Add empty fields that will be set in the next step
  '' filename,
  '' file_id,
  first(CASE WHEN p.fileMd5='' THEN NULL ELSE p.fileMd5 END) file_md5,
  count(DISTINCT p.fileMd5) num_file_md5,
  first(CASE WHEN p.fileSha2='' THEN NULL ELSE p.fileSha2 END) file_sha2,
  count(DISTINCT p.fileSha2) num_file_sha2,
  min(CASE WHEN p.ActivityType IN ('START','POLLED') THEN win32_to_epoch(p.EventTime) ELSE NULL END) process_started_seconds,
  to_timestamp_micros(process_started_seconds) process_started,
  to_timestamp_micros(win32_to_epoch(min(p.EventTime))) first_seen,
  to_timestamp_micros(win32_to_epoch(max(p.EventTime))) last_seen,
  count(*) num_start_events
FROM raw_process p
GROUP BY p.pidhash
;

--== Update process_path
UPDATE process_tmp 
  SET filename=
  CASE 
    WHEN process_path IS NULL THEN process_name
    WHEN substring(process_path,-1)='\\' THEN concat(process_path, process_name)
  ELSE process_path
  END
WHERE process_path IS NOT NULL OR process_name IS NOT NULL
;    

--== Set file_id on process    
UPDATE process_tmp 
  SET file_id=
	  md5(concat_ws('||',hostname,filename))
WHERE filename IS NOT NULL 
;

CREATE TABLE IF NOT EXISTS process
AS
SELECT
	p.*,
	s.* EXCLUDE (pidhash)
FROM
	process_tmp p
LEFT OUTER JOIN (
	SELECT
		pidhash,
		to_timestamp_micros(win32_to_epoch(max(EventTime))) process_term_seconds,
		to_timestamp_micros(win32_to_epoch(max(EventTime))) process_term,
		max(CPUCycleCount) cpu_cycle_count,
		max(CPUUtilization) cpu_utilization,
		max(CommitCharge) commit_charge,
		max(CommitPeak) commit_peak,
		max(ReadOperationCount) read_operation_count,
		max(WriteOperationCount) write_operation_count,
		max(ReadTransferKiloBytes) read_transfer_kilobytes,
		max(WriteTransferKiloBytes) write_transfer_kilobytes,
		max(HardFaultCount) hard_fault_count,
		max(TokenElevationType) token_elevation_type,
		max(ExitCode) exit_code,
		count(*) num_process_stop
	FROM
		RAW_PROCESS_STOP
		--	WHERE dayPk=20221227
	GROUP BY
		PidHash) s ON
	p.pid_hash = s.pidhash
;

CREATE table if not exists process_conn_incr
AS
SELECT
  'windows' os_family,
  hostname hostname,
  pidhash pid_hash,
  connid conn_id,
  protocol protocol,
  -- Calculate a time range for grouping. Starting with 1 minute.
  -- Can't use the UDF in a view, so here's the direct version:
  to_timestamp_micros(floor(win32_to_epoch(FirstSeenMs)/60)*60) incr_start,
--    int(((FirstSeenMs / 1e7) - 11644473600)/60)*60 incr_start_secs,
  --localiphash,
  -- Can't use a UDF in a view, so dotted-quad IPs are done in the table building.
  int_to_ip(cast(localipaddr as bigint)) local_ip_addr,
  localipaddr local_ip_int,
  localport local_port,
  localipprivategateway local_pg,
  --remoteiphash,
  int_to_ip(cast(remoteipaddr as bigint)) remote_ip_addr,
  remoteipaddr remote_ip_int,
  remoteport remote_port,
  remoteipprivategateway remote_pg,
  sum(eventCount) total_events,
  sum(packetSize) total_size,
  count(*) num_raw_rows,
  sum(CASE WHEN ipEvent='TcpIp/Accept' THEN eventCount END) tcp_accept_count,
  sum(CASE WHEN ipEvent='TcpIp/Connect' THEN eventCount END) tcp_Connect_count,
  sum(CASE WHEN ipEvent='TcpIp/Disconnect' THEN eventCount END) tcp_disconnect_count,
  sum(CASE WHEN ipEvent='TcpIp/Reconnect' THEN eventCount END) tcp_reconnect_count,
  sum(CASE WHEN ipEvent='TcpIp/Recv' THEN eventCount END) tcp_recv_count,
  sum(CASE WHEN ipEvent='TcpIp/Recv' THEN packetSize END) tcp_recv_size,
  sum(CASE WHEN ipEvent='TcpIp/Retransmit' THEN eventCount END) tcp_Retransmit_count,
  sum(CASE WHEN ipEvent='TcpIp/Send' THEN eventCount END) tcp_send_count,
  sum(CASE WHEN ipEvent='TcpIp/Send' THEN packetSize END) tcp_send_size,
  sum(CASE WHEN ipEvent='TcpIp/TCPCopy' THEN eventCount END) tcp_TCPCopy_count,
  sum(CASE WHEN ipEvent='TcpIp/TCPCopy' THEN packetSize END) tcp_TCPCopy_size,
  sum(CASE WHEN ipEvent='UdpIp/Recv' THEN eventCount END) udp_recv_count,
  sum(CASE WHEN ipEvent='UdpIp/Recv' THEN packetSize END) udp_recv_size,
  sum(CASE WHEN ipEvent='UdpIp/Send' THEN eventCount END) udp_send_count,
  sum(CASE WHEN ipEvent='UdpIp/Send' THEN packetSize END) udp_send_size,
  -- Gather some basic stats on traffic
  -- Total events/sizes. In practice, at this level of detail, these *should* be really close the TCP/UDP stats, but exceptions like many connects/disconnects, retransmits, could throw some off.
  min(eventcount) min_10sec_eventCount,
  max(eventcount) max_10sec_eventCount,
  -- These might need to be fixed. Clarify...
  min(minPacketSize) min_size,
  max(maxPacketSize) max_size,
  sum(packetSizeSquared) sq_size,
  -- TCP Stats - only doing send/receive for now.
  max(CASE WHEN ipEvent='TcpIp/Recv' THEN eventCount END) max_tcp_recv_count,
  min(CASE WHEN ipEvent='TcpIp/Recv' THEN minPacketSize END) min_tcp_recv_size,
  max(CASE WHEN ipEvent='TcpIp/Recv' THEN maxPacketSize END) max_tcp_recv_size,
  sum(CASE WHEN ipEvent='TcpIp/Recv' THEN packetSizeSquared END) sq_tcp_recv_size,
  --
  max(CASE WHEN ipEvent='TcpIp/Send' THEN eventCount END) max_tcp_send_count,
  min(CASE WHEN ipEvent='TcpIp/Send' THEN minPacketSize END) min_tcp_send_size,
  max(CASE WHEN ipEvent='TcpIp/Send' THEN maxPacketSize END) max_tcp_send_size,
  sum(CASE WHEN ipEvent='TcpIp/Send' THEN packetSizeSquared END) sq_tcp_send_size,
  -- UDP Stats
  max(CASE WHEN ipEvent='UdpIp/Recv' THEN eventCount END) max_udp_recv_count,
  min(CASE WHEN ipEvent='UdpIp/Recv' THEN minPacketSize END) min_udp_recv_size,
  max(CASE WHEN ipEvent='UdpIp/Recv' THEN maxPacketSize END) max_udp_recv_size,
  sum(CASE WHEN ipEvent='UdpIp/Recv' THEN packetSizeSquared END) sq_udp_recv_size,
  --
  max(CASE WHEN ipEvent='UdpIp/Send' THEN eventCount END) max_udp_send_count,
  min(CASE WHEN ipEvent='UdpIp/Send' THEN minPacketSize END) min_udp_send_size,
  max(CASE WHEN ipEvent='UdpIp/Send' THEN maxPacketSize END) max_udp_send_size,
  sum(CASE WHEN ipEvent='UdpIp/Send' THEN packetSizeSquared END) sq_udp_send_size,  
  to_timestamp_micros(win32_to_epoch(min(firstSeenMS))) first_seen,
  to_timestamp_micros(win32_to_epoch(max(lastSeenMS))) last_seen
FROM RAW_PROCESS_CONN_INCR
GROUP BY all
;

CREATE TABLE IF NOT EXISTS process_net_conn
AS
SELECT 
  os_family,
  hostname,
  pid_hash,
  conn_id,
  protocol,
  local_ip_addr,
  local_port,
  local_pg,
  remote_ip_addr,
  remote_port,
  remote_pg,
  sum(total_events) total_events,
  sum(total_size) total_size,
  sum(sq_size) sq_size,
  sum(num_raw_rows) num_raw_rows,
  sum(tcp_accept_count) tcp_accept_count,
  sum(tcp_Connect_count) tcp_Connect_count,
  sum(tcp_disconnect_count) tcp_disconnect_count,
  sum(tcp_reconnect_count) tcp_reconnect_count,
  sum(tcp_recv_count) tcp_recv_count,
  sum(tcp_recv_size) tcp_recv_size,
  sum(sq_tcp_recv_size) sq_tcp_recv_size,
  sum(tcp_Retransmit_count) tcp_Retransmit_count,
  sum(tcp_send_count) tcp_send_count,
  sum(tcp_send_size) tcp_send_size,
  sum(sq_tcp_send_size) sq_tcp_send_size,
  sum(tcp_TCPCopy_count) tcp_TCPCopy_count,
  sum(tcp_TCPCopy_size) tcp_TCPCopy_size,
  sum(udp_recv_count) udp_recv_count,
  sum(udp_recv_size) udp_recv_size,
  sum(sq_udp_recv_size) sq_udp_recv_size,
  sum(udp_send_count) udp_send_count,
  sum(udp_send_size) udp_send_size,
  sum(sq_udp_send_size) sq_udp_send_size,
  min(first_seen) first_seen,
  max(last_seen) last_seen
FROM PROCESS_CONN_INCR
GROUP BY ALL
;

CREATE TABLE IF NOT EXISTS process_net_summary
AS
SELECT
  os_family,
  pid_hash,  
  hostname,
  count(DISTINCT conn_id) conn_id_count,
  sum(total_events) net_total_events,
  sum(total_size) net_total_size,
  sum(num_raw_rows) num_raw_rows,
  sum(tcp_accept_count) tcp_accept_count,
  sum(tcp_Connect_count) tcp_Connect_count,
  sum(tcp_disconnect_count) tcp_disconnect_count,
  sum(tcp_reconnect_count) tcp_reconnect_count,
  sum(tcp_recv_count) tcp_recv_count,
  sum(tcp_recv_size) tcp_recv_size,
  sum(tcp_Retransmit_count) tcp_Retransmit_count,
  sum(tcp_send_count) tcp_send_count,
  sum(tcp_send_size) tcp_send_size,
  sum(tcp_TCPCopy_count) tcp_tcpcopy_count,
  sum(tcp_TCPCopy_size) tcp_tcpcopy_size,
  sum(udp_recv_count) udp_recv_count,
  sum(udp_recv_size) udp_recv_size,
  sum(udp_send_count) udp_send_count,
  sum(udp_send_size) udp_send_size,
  min(first_seen) first_seen,
  max(last_seen) last_seen,
  -- Communication Metrics TCP/UDP. Do we need packet counts also?
  sum(ifnull(tcp_recv_size,0)+ifnull(udp_recv_size,0)) net_recv_size,
  sum(ifnull(tcp_send_size,0)+ifnull(udp_send_size,0)) net_send_size,
  sum((ifnull(tcp_recv_size,0)+ifnull(udp_recv_size,0))+(ifnull(tcp_send_size,0)+ifnull(udp_send_size,0))) net_rs_total,
  (
        sum(ifnull(tcp_send_size,0)+ifnull(udp_send_size,0))/
          sum(
                (ifnull(tcp_recv_size,0)+ifnull(udp_recv_size,0))+
                (ifnull(tcp_send_size,0)+ifnull(udp_send_size,0))
          )
  ) net_send_vs_recv,
  -- Communication Metrics TCP
  -- Do we need tcp_total_size? Need to check other TCP event numbers and see if it really makes sense.
  sum(ifnull(tcp_recv_size,0)+ifnull(tcp_send_size,0)) tcp_rs_total,
  (
        sum(ifnull(tcp_send_size,0))/
          sum(
                (ifnull(tcp_recv_size,0))+(ifnull(tcp_send_size,0))
          )
  ) tcp_send_vs_recv,
  -- Communication Metrics UDP
  -- For UDP, we definitely don't need both udp_total_size and udp_rs_total as there are only SEND/RECV types.
  sum(ifnull(udp_recv_size,0)+ifnull(udp_send_size,0)) udp_rs_total,
  (
        sum(ifnull(udp_send_size,0))/
          sum(
                (ifnull(udp_recv_size,0))+(ifnull(udp_send_size,0))
          )
  ) udp_send_vs_recv,
	  -- Summary Statistics
	min(TOTAL_SIZE) min_bytes,
	max(TOTAL_SIZE) max_bytes,
	avg(TOTAL_SIZE) avg_bytes,
	min(total_events) min_packets,
	max(total_events) max_packets,
	avg(total_events) avg_packets,
	sum(sq_size) sq_size
FROM  process_net_conn
GROUP BY 1,2,3
;

-- Summarize file activity to PID_HASH+FILE_HASH
CREATE TABLE IF NOT EXISTS process_file
AS 
SELECT 
  hostname hostname,
  pidhash pid_hash,
  -- generate FileID
  md5(concat_ws('||',hostname,file_path)) file_id,
  file_hash file_hash,
  file_path filename,
  activitytype activity_type,
  sum(bytesRequested) bytes_requested,
  sum(eventCount) event_count,
  count(*) num_raw_rows,
  to_timestamp_micros(win32_to_epoch(min(firstSeen))) first_seen,
  to_timestamp_micros(win32_to_epoch(max(lastSeen))) last_seen,
  to_timestamp(min(EventTime)) min_event,
  to_timestamp(max(EventTime)) max_event
FROM raw_process_file
GROUP BY ALL
;

-- Summarize registry event increments.
-- Should there be multiple levels of summary?
CREATE TABLE IF NOT EXISTS process_registry
as
SELECT 
  hosthame hostname,
  pidhash pid_hash,
  reg_path reg_path,
  reg_value reg_value,
  ActivityType activity_type,
  reg_data reg_data,
  sum(eventCount) event_count,
  count(*) num_raw_rows,
--  win32_to_epoch(min(FirstSeenMs)) first_seen_seconds,
  --win32_to_epoch(max(lastseenms)) last_seen_seconds
  to_timestamp_micros(win32_to_epoch(min(FirstSeenMs))) first_seen,
  to_timestamp_micros(win32_to_epoch(max(lastseenms))) last_seen,
  to_timestamp(min(EventTime)) min_event,
  to_timestamp(max(EventTime)) max_event
FROM  RAW_PROCESS_REGISTRY
GROUP BY ALL
;

-- Summarize to PID_HASH+FILENAME
CREATE TABLE IF NOT EXISTS process_image_load
AS
SELECT 
  computername hostname,
  pidhash pid_hash,
  md5(concat_ws('||',computername,lower(filename))) file_id,
  lower(filename) filename,
  max(buildTime) build_time,
  count(DISTINCT buildTime) num_uniq_build_times,
  max(imageChecksum) checksum,
  count(DISTINCT imageChecksum) num_uniq_checksums,
  max(defaultbase) default_base,
  count(DISTINCT defaultbase) num_default_base,
  max(imagebase) image_base,
  count(DISTINCT imagebase) num_image_base,
--  max(md5) md5,
--  count(DISTINCT MD5) num_md5,
  min(imageSize) min_image_size,
  max(imageSize) max_image_size,
  count(distinct imageSize) num_image_size,
  sum(IF(activitytype='Load',1,0)) num_load,
  sum(IF(activitytype='Unload',1,0)) num_unload,
  to_timestamp_micros(win32_to_epoch(min(eventtime))) first_seen,
  to_timestamp_micros(win32_to_epoch(max(eventtime))) last_seen
FROM raw_imageload
GROUP BY ALL
;

CREATE OR REPLACE VIEW process_exe_file_summary
AS
SELECT 'process' AS SOURCE,
  hostname,
  filename,
  file_id,
  min(process_started) min_process_started,
  max(process_term) max_process_term,
  count(*) AS process_num_rows
FROM process 
GROUP BY ALL
;

CREATE OR REPLACE VIEW files_tmp_v1
AS
SELECT
  file_id,
  hostname,	
  filename,
  process_num_rows,
  CAST(NULL AS integer) dll_num_rows,
  CAST(NULL AS integer) file_num_rows,
  min_process_started,
  max_process_term,
  CAST(NULL AS timestamp) dll_first_seen,
  CAST(NULL AS timestamp) dll_last_seen,
  CAST(NULL AS timestamp) file_first_seen,
  CAST(NULL AS timestamp) file_last_seen
FROM process_exe_file_summary
union
SELECT 
  file_id,
  hostname,
  filename,
  NULL AS process_num_rows,
  count(*) AS dll_num_rows,
  NULL AS file_num_rows,
  NULL AS min_process_started,
  NULL AS max_process_term,
  min(first_seen) dll_first_seen,
  max(last_seen) dll_last_seen,
  NULL AS file_first_seen,
  NULL AS file_last_seen
FROM PROCESS_IMAGE_LOAD GROUP BY 1,2,3,4
union
SELECT
  file_id,
  hostname,
  filename,
  NULL AS process_num_rows,
  NULL AS dll_num_rows,
  count(*) AS file_num_rows,
  NULL AS min_processstarted,
  NULL AS max_processterm,
  NULL AS dll_first_seen,
  NULL AS dll_last_seen,
  min(first_seen) file_first_seen,
  max(last_seen) file_last_seen
FROM PROCESS_FILE GROUP BY 1,2,3
;

CREATE table files
as
SELECT 
  file_id,
  hostname,
  filename,
  sum(process_num_rows) process_num_rows,
  sum(dll_num_rows) dll_num_rows,
  sum(file_num_rows) file_num_rows,
  min(min_process_started) min_process_started,
  max(max_process_term) max_process_term,
  min(dll_first_seen) dll_first_seen,
  max(dll_last_seen) dll_last_seen,
  min(file_first_seen) file_first_seen,
  max(file_last_seen) file_last_seen
FROM files_tmp_v1
GROUP BY 1,2,3
;

-- Create a set of files using just the filename.
CREATE TABLE all_files
AS 
SELECT filename,
  count(DISTINCT hostname) num_hosts,
  sum(process_num_rows) process_num_rows,
  sum(dll_num_rows) dll_num_rows,
  sum(file_num_rows) file_num_rows
FROM FILES 
GROUP BY 1
;

