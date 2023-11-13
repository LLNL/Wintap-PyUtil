/*
 * Create the standard view tables from existing RAW tables.
 * SQL Dialect: DuckDB
 *
 */

/* TODO: Change first() to mode(), which returns the most frequent value */

CREATE TABLE IF NOT EXISTS host
AS
SELECT
 -- Ignore host.id.hash for now as it is really only composed of the hostname.
 -- host.hostid.hash,
 hostname hostname,
 first('windows') os_family,
 to_timestamp(min(eventtime)) first_seen, -- TODO: Should lastseen for host just be dropped? It isn't accurate. Better would be to derive it from other data: network, file, etc.
 to_timestamp(max(eventtime)) last_seen, 
 first(os) os, 
 count(DISTINCT os) num_os, 
 first(CASE 
           WHEN osversion='' THEN NULL 
           ELSE osversion 
       END) os_version, 
 count(DISTINCT osversion) num_os_version, 
 first(CASE 
           WHEN arch='' THEN NULL
           ELSE arch
       END) arch,
 count(DISTINCT arch) num_arch,
 first(processorcount) processor_count,
 count(DISTINCT processorcount) num_processor_count,
 first(processorspeed) processor_speed,
 count(DISTINCT processorspeed) num_processor_speed,
 first(hasbattery) has_battery,
 count(DISTINCT hasbattery) num_has_battery,
 first(DOMAIN) ad_domain,
 count(DISTINCT DOMAIN) num_ad_domain,
 first(domainrole) domain_role,
 count(DISTINCT domainrole) num_domain_role,
 max(lastboot) last_boot,
 count(DISTINCT lastboot) num_last_boot,
 max(wintapversion) wintap_version,
 count(DISTINCT wintapversion) num_wintap_version,
 max(etlversion) etl_version,
 count(DISTINCT etlversion) num_etl_version,
 max(collectors) collectors,
 count(DISTINCT collectors) num_collectors,
 count(*) num_rows
FROM raw_host
GROUP BY 1
;


CREATE TABLE IF NOT EXISTS host_ip
AS
SELECT hostname,
       first('windows') os_family,
       CASE
           WHEN privategateway='' THEN NULL
           ELSE privategateway
       END private_gateway,
       int_to_ip(cast(ipaddr
AS bigint)) ip_addr_no,
       CASE
           WHEN mac='' THEN NULL
           ELSE mac
       END mac,
       ipaddr ip_addr,
       interface,
       mtu,
       to_timestamp(min(eventtime)) first_seen,
       to_timestamp(max(eventtime)) last_seen,
       count(*) num_rows,
FROM raw_macip
GROUP BY ALL
;

-- Summarize Process events into Process entities

CREATE TABLE IF NOT EXISTS process_tmp
AS
SELECT p.pidhash pid_hash, -- osfamily will eventually come back as a partition key
 first('windows') os_family,
 first(p.hostname) hostname,
 first(pid) os_pid,
 first(CASE
           WHEN p.processname='' THEN NULL
           ELSE p.processname
       END) process_name,
 count(DISTINCT p.processname) num_process_name,
 first(CASE
           WHEN p.processargs='' THEN NULL
           ELSE p.processargs
       END) args,
 count(DISTINCT p.processargs) num_args, -- Ignore useless names
 first(CASE
           WHEN p.username=''
                OR lower(p.username)='na' THEN NULL
           ELSE p.username
       END) user_name,
 count(DISTINCT p.username) num_user_name,
 first(CASE
           WHEN p.parentpidhash='' THEN NULL
           ELSE p.parentpidhash
       END) parent_pid_hash,
 count(DISTINCT p.parentpidhash) num_parent_pid_hash,
 first(p.parentpid) parent_os_pid,
 count(DISTINCT p.parentpid) num_parent_os_pid,
 first(CASE
           WHEN p.processpath='' THEN NULL
           ELSE p.processpath
       END) process_path,
 count(DISTINCT p.processpath) num_process_path, -- Add empty fields that will be set in the next step
 '' filename,
    '' file_id,
       first(CASE
                 WHEN p.filemd5='' THEN NULL
                 ELSE p.filemd5
             END) file_md5,
       count(DISTINCT p.filemd5) num_file_md5,
       first(CASE
                 WHEN p.filesha2='' THEN NULL
                 ELSE p.filesha2
             END) file_sha2,
       count(DISTINCT p.filesha2) num_file_sha2,
       min(CASE
               WHEN p.activitytype IN ('START', 'POLLED') THEN win32_to_epoch(p.EventTime)
               ELSE NULL
           END) process_started_seconds,
       to_timestamp_micros(process_started_seconds) process_started,
       to_timestamp_micros(win32_to_epoch(min(p.eventtime))) first_seen,
       to_timestamp_micros(win32_to_epoch(max(p.eventtime))) last_seen,
       count(*) num_start_events
FROM raw_process p
GROUP BY p.pidhash
;

--== Update process_path

UPDATE process_tmp
SET filename= CASE
                  WHEN process_path IS NULL THEN process_name
                  WHEN substring(process_path, -1)='\\' THEN concat(process_path, process_name)
                  ELSE process_path
              END
WHERE process_path IS NOT NULL
  OR process_name IS NOT NULL
;

--== Set file_id on process

UPDATE process_tmp
SET file_id= md5(concat_ws('||', hostname, filename))
WHERE filename IS NOT NULL
;


CREATE TABLE IF NOT EXISTS process
AS
SELECT p.*,
       s.* exclude (pidhash)
FROM process_tmp p
LEFT OUTER JOIN
  (SELECT pidhash,
          to_timestamp_micros(win32_to_epoch(max(eventtime))) process_term_seconds,
          to_timestamp_micros(win32_to_epoch(max(eventtime))) process_term,
          max(cpucyclecount) cpu_cycle_count,
          max(cpuutilization) cpu_utilization,
          max(commitcharge) commit_charge,
          max(commitpeak) commit_peak,
          max(readoperationcount) read_operation_count,
          max(writeoperationcount) write_operation_count,
          max(readtransferkilobytes) read_transfer_kilobytes,
          max(writetransferkilobytes) write_transfer_kilobytes,
          max(hardfaultcount) hard_fault_count,
          max(tokenelevationtype) token_elevation_type,
          max(exitcode) exit_code,
          count(*) num_process_stop
   FROM raw_process_stop --	WHERE dayPk=20221227

   GROUP BY pidhash) s ON p.pid_hash = s.pidhash
;


CREATE TABLE IF NOT EXISTS process_conn_incr
AS
SELECT 'windows' os_family,
                 hostname hostname,
                 pidhash pid_hash,
                 processname process_name,
                 connid conn_id,
                 protocol protocol, -- Calculate a time range for grouping. Starting with 1 minute.
 -- Can't use the UDF in a view, so here's the direct version:
 to_timestamp_micros(floor(win32_to_epoch(FirstSeenMs)/60)*60) incr_start, --    int(((FirstSeenMs / 1e7) - 11644473600)/60)*60 incr_start_secs,
 --localiphash,
 -- Can't use a UDF in a view, so dotted-quad IPs are done in the table building.
 int_to_ip(cast(localipaddr
AS bigint)) local_ip_addr, 
 localipaddr local_ip_int, 
 localport local_port, 
 localipprivategateway local_pg, --remoteiphash,
 int_to_ip(cast(remoteipaddr
AS bigint)) remote_ip_addr, 
 remoteipaddr remote_ip_int, 
 remoteport remote_port, 
 remoteipprivategateway remote_pg, 
 sum(eventcount) total_events, 
 sum(packetsize) total_size, 
 count(*) num_raw_rows, 
 sum(CASE 
         WHEN ipevent='TcpIp/Accept' THEN eventcount 
     END) tcp_accept_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/Connect' THEN eventcount 
     END) tcp_connect_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/Disconnect' THEN eventcount 
     END) tcp_disconnect_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/Reconnect' THEN eventcount 
     END) tcp_reconnect_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/Recv' THEN eventcount 
     END) tcp_recv_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/Recv' THEN packetsize 
     END) tcp_recv_size, 
 sum(CASE 
         WHEN ipevent='TcpIp/Retransmit' THEN eventcount 
     END) tcp_retransmit_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/Send' THEN eventcount 
     END) tcp_send_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/Send' THEN packetsize 
     END) tcp_send_size, 
 sum(CASE 
         WHEN ipevent='TcpIp/TCPCopy' THEN eventcount 
     END) tcp_tcpcopy_count, 
 sum(CASE 
         WHEN ipevent='TcpIp/TCPCopy' THEN packetsize 
     END) tcp_tcpcopy_size, 
 sum(CASE 
         WHEN ipevent='UdpIp/Recv' THEN eventcount 
     END) udp_recv_count, 
 sum(CASE 
         WHEN ipevent='UdpIp/Recv' THEN packetsize 
     END) udp_recv_size, 
 sum(CASE 
         WHEN ipevent='UdpIp/Send' THEN eventcount 
     END) udp_send_count, 
 sum(CASE 
         WHEN ipevent='UdpIp/Send' THEN packetsize 
     END) udp_send_size, -- Gather some basic stats on traffic
 -- Total events/sizes. In practice, at this level of detail, these *should* be really close the TCP/UDP stats, but exceptions like many connects/disconnects, retransmits, could throw some off.
 min(eventcount) min_10sec_eventcount, 
 max(eventcount) max_10sec_eventcount, -- These might need to be fixed. Clarify...
 min(minpacketsize) min_size, 
 max(maxpacketsize) max_size, 
 sum(packetsizesquared) sq_size, -- TCP Stats - only doing send/receive for now.
 max(CASE 
         WHEN ipevent='TcpIp/Recv' THEN eventcount 
     END) max_tcp_recv_count, 
 min(CASE 
         WHEN ipevent='TcpIp/Recv' THEN minpacketsize 
     END) min_tcp_recv_size, 
 max(CASE 
         WHEN ipevent='TcpIp/Recv' THEN maxpacketsize 
     END) max_tcp_recv_size, 
 sum(CASE 
         WHEN ipevent='TcpIp/Recv' THEN packetsizesquared 
     END) sq_tcp_recv_size, --
 max(CASE 
         WHEN ipevent='TcpIp/Send' THEN eventcount 
     END) max_tcp_send_count, 
 min(CASE 
         WHEN ipevent='TcpIp/Send' THEN minpacketsize 
     END) min_tcp_send_size, 
 max(CASE 
         WHEN ipevent='TcpIp/Send' THEN maxpacketsize 
     END) max_tcp_send_size, 
 sum(CASE 
         WHEN ipevent='TcpIp/Send' THEN packetsizesquared 
     END) sq_tcp_send_size, -- UDP Stats
 max(CASE 
         WHEN ipevent='UdpIp/Recv' THEN eventcount 
     END) max_udp_recv_count, 
 min(CASE 
         WHEN ipevent='UdpIp/Recv' THEN minpacketsize 
     END) min_udp_recv_size, 
 max(CASE 
         WHEN ipevent='UdpIp/Recv' THEN maxpacketsize 
     END) max_udp_recv_size, 
 sum(CASE 
         WHEN ipevent='UdpIp/Recv' THEN packetsizesquared 
     END) sq_udp_recv_size, --
 max(CASE 
         WHEN ipevent='UdpIp/Send' THEN eventcount 
     END) max_udp_send_count, 
 min(CASE 
         WHEN ipevent='UdpIp/Send' THEN minpacketsize 
     END) min_udp_send_size, 
 max(CASE 
         WHEN ipevent='UdpIp/Send' THEN maxpacketsize 
     END) max_udp_send_size, 
 sum(CASE 
         WHEN ipevent='UdpIp/Send' THEN packetsizesquared
     END) sq_udp_send_size,
 to_timestamp_micros(win32_to_epoch(min(FirstSeenMs))) first_seen,
 to_timestamp_micros(win32_to_epoch(max(lastseenms))) last_seen
FROM raw_process_conn_incr
GROUP BY ALL
;


CREATE TABLE IF NOT EXISTS process_net_conn
AS
SELECT os_family,
       hostname,
       pid_hash,
       process_name,
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
       sum(tcp_connect_count) tcp_connect_count,
       sum(tcp_disconnect_count) tcp_disconnect_count,
       sum(tcp_reconnect_count) tcp_reconnect_count,
       sum(tcp_recv_count) tcp_recv_count,
       sum(tcp_recv_size) tcp_recv_size,
       sum(sq_tcp_recv_size) sq_tcp_recv_size,
       sum(tcp_retransmit_count) tcp_retransmit_count,
       sum(tcp_send_count) tcp_send_count,
       sum(tcp_send_size) tcp_send_size,
       sum(sq_tcp_send_size) sq_tcp_send_size,
       sum(tcp_tcpcopy_count) tcp_tcpcopy_count,
       sum(tcp_tcpcopy_size) tcp_tcpcopy_size,
       sum(udp_recv_count) udp_recv_count,
       sum(udp_recv_size) udp_recv_size,
       sum(sq_udp_recv_size) sq_udp_recv_size,
       sum(udp_send_count) udp_send_count,
       sum(udp_send_size) udp_send_size,
       sum(sq_udp_send_size) sq_udp_send_size,
       min(first_seen) first_seen,
       max(last_seen) last_seen
FROM process_conn_incr
GROUP BY ALL
;


CREATE TABLE IF NOT EXISTS process_net_summary
AS
SELECT os_family,
       pid_hash,
       process_name,
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
       max(last_seen) last_seen, -- Communication Metrics TCP/UDP. Do we need packet counts also?
 sum(ifnull(tcp_recv_size, 0)+ifnull(udp_recv_size, 0)) net_recv_size,
 sum(ifnull(tcp_send_size, 0)+ifnull(udp_send_size, 0)) net_send_size,
 sum((ifnull(tcp_recv_size, 0)+ifnull(udp_recv_size, 0))+(ifnull(tcp_send_size, 0)+ifnull(udp_send_size, 0))) net_rs_total,
 (sum(ifnull(tcp_send_size, 0)+ifnull(udp_send_size, 0))/ sum((ifnull(tcp_recv_size, 0)+ifnull(udp_recv_size, 0))+ (ifnull(tcp_send_size, 0)+ifnull(udp_send_size, 0)))) net_send_vs_recv, -- Communication Metrics TCP
 -- Do we need tcp_total_size? Need to check other TCP event numbers and see if it really makes sense.
 sum(ifnull(tcp_recv_size, 0)+ifnull(tcp_send_size, 0)) tcp_rs_total,
 (sum(ifnull(tcp_send_size, 0))/ sum((ifnull(tcp_recv_size, 0))+(ifnull(tcp_send_size, 0)))) tcp_send_vs_recv, -- Communication Metrics UDP
 -- For UDP, we definitely don't need both udp_total_size and udp_rs_total as there are only SEND/RECV types.
 sum(ifnull(udp_recv_size, 0)+ifnull(udp_send_size, 0)) udp_rs_total,
 (sum(ifnull(udp_send_size, 0))/ sum((ifnull(udp_recv_size, 0))+(ifnull(udp_send_size, 0)))) udp_send_vs_recv, -- Summary Statistics
 min(total_size) min_bytes,
 max(total_size) max_bytes,
 avg(total_size) avg_bytes,
 min(total_events) min_packets,
 max(total_events) max_packets,
 avg(total_events) avg_packets,
 sum(sq_size) sq_size
FROM process_net_conn
GROUP BY 1,
         2,
         3
;

-- Summarize file activity to PID_HASH+FILE_HASH

CREATE TABLE IF NOT EXISTS process_file
AS
SELECT hostname hostname,
       pidhash pid_hash, -- generate FileID
       processname process_name,
 md5(concat_ws('||', hostname, file_path)) file_id,
 file_hash file_hash,
 file_path filename,
 activitytype activity_type,
 sum(bytesrequested) bytes_requested,
 sum(eventcount) event_count,
 count(*) num_raw_rows,
 to_timestamp_micros(win32_to_epoch(min(firstseen))) first_seen,
 to_timestamp_micros(win32_to_epoch(max(lastseen))) last_seen,
 to_timestamp(min(eventtime)) min_event,
 to_timestamp(max(eventtime)) max_event
FROM raw_process_file
GROUP BY ALL
;

-- Summarize registry event increments.
-- Should there be multiple levels of summary?

CREATE TABLE IF NOT EXISTS process_registry
AS
SELECT hosthame hostname,
       pidhash pid_hash,
       processname process_name,
       reg_path reg_path,
       reg_value reg_value,
       activitytype activity_type,
       reg_data reg_data,
       sum(eventcount) event_count,
       count(*) num_raw_rows, --  win32_to_epoch(min(FirstSeenMs)) first_seen_seconds,
 --win32_to_epoch(max(lastseenms)) last_seen_seconds
 to_timestamp_micros(win32_to_epoch(min(firstseenms))) first_seen,
 to_timestamp_micros(win32_to_epoch(max(lastseenms))) last_seen,
 to_timestamp(min(eventtime)) min_event,
 to_timestamp(max(eventtime)) max_event
FROM raw_process_registry
GROUP BY ALL
;

-- Summarize to PID_HASH+FILENAME

CREATE TABLE IF NOT EXISTS process_image_load
AS
SELECT computername hostname,
       pidhash pid_hash,
        processname process_name,
       md5(concat_ws('||', computername, lower(filename))) file_id,
       lower(filename) filename,
       max(buildtime) build_time,
       count(DISTINCT buildtime) num_uniq_build_times,
       max(imagechecksum) checksum,
       count(DISTINCT imagechecksum) num_uniq_checksums,
       max(defaultbase) default_base,
       count(DISTINCT defaultbase) num_default_base,
       max(imagebase) image_base,
       count(DISTINCT imagebase) num_image_base, --  max(md5) md5,
--  count(DISTINCT MD5) num_md5,
 min(imagesize) min_image_size,
 max(imagesize) max_image_size,
 count(DISTINCT imagesize) num_image_size,
 sum(if(activitytype='Load', 1, 0)) num_load,
 sum(if(activitytype='Unload', 1, 0)) num_unload,
 to_timestamp_micros(win32_to_epoch(min(eventtime))) first_seen,
 to_timestamp_micros(win32_to_epoch(max(eventtime))) last_seen
FROM raw_imageload
GROUP BY ALL
;


CREATE OR REPLACE VIEW process_exe_file_summary
AS
SELECT 'process'
AS SOURCE,
       hostname,
       filename,
       file_id,
       min(process_started) min_process_started,
       max(process_term) max_process_term,
       count(*)
AS process_num_rows
FROM process
GROUP BY ALL
;


CREATE OR REPLACE VIEW files_tmp_v1
AS
SELECT file_id,
       hostname,
       filename,
       process_num_rows,
       cast(NULL
AS integer) dll_num_rows,
       cast(NULL
AS integer) file_num_rows,
       min_process_started,
       max_process_term,
       cast(NULL
AS timestamp) dll_first_seen,
       cast(NULL
AS timestamp) dll_last_seen,
       cast(NULL
AS timestamp) file_first_seen,
       cast(NULL
AS timestamp) file_last_seen
FROM process_exe_file_summary
UNION
SELECT file_id,
       hostname,
       filename,
       NULL
AS process_num_rows,
       count(*)
AS dll_num_rows,
       NULL
AS file_num_rows,
       NULL
AS min_process_started,
       NULL
AS max_process_term,
       min(first_seen) dll_first_seen,
       max(last_seen) dll_last_seen,
       NULL
AS file_first_seen,
       NULL
AS file_last_seen
FROM process_image_load
GROUP BY 1,
         2,
         3,
         4
UNION
SELECT file_id,
       hostname,
       filename,
       NULL
AS process_num_rows,
       NULL
AS dll_num_rows,
       count(*)
AS file_num_rows,
       NULL
AS min_processstarted,
       NULL
AS max_processterm,
       NULL
AS dll_first_seen,
       NULL
AS dll_last_seen,
       min(first_seen) file_first_seen,
       max(last_seen) file_last_seen
FROM process_file
GROUP BY 1,
         2,
         3
;


CREATE TABLE files
AS
SELECT file_id,
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
GROUP BY 1,
         2,
         3
;

-- Create a set of files using just the filename.

CREATE TABLE all_files
AS
SELECT filename,
       count(DISTINCT hostname) num_hosts,
       sum(process_num_rows) process_num_rows,
       sum(dll_num_rows) dll_num_rows,
       sum(file_num_rows) file_num_rows
FROM files
GROUP BY 1
;

-- Generate Process Trees. There *should* be 1 graph per unique NTOSKRNL.EXE process.

CREATE TABLE process_tree
AS WITH RECURSIVE process_tree (hostname, pid_hash, os_pid, process_name, process_path, LEVEL, parent_pid_hash, parent_os_pid, seq)
AS
  (SELECT hostname,
          pid_hash,
          os_pid,
          process_name,
          process_path,
          0,
          parent_pid_hash,
          parent_os_pid,
          1
   FROM process
   WHERE pid_hash=parent_pid_hash
   UNION ALL SELECT p.hostname,
                    p.pid_hash,
                    p.os_pid,
                    p.process_name,
                    p.process_path,
                    pt.level + 1,
                    pt.pid_hash,
                    pt.os_pid,
                    pt.seq
   FROM process p,
        process_tree pt
   WHERE p.parent_pid_hash = pt.pid_hash
     AND p.parent_pid_hash <> p.pid_hash )
SELECT *
FROM process_tree
ORDER BY LEVEL,
         parent_pid_hash,
         seq
;