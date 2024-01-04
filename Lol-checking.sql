create table process_all as select * from parquet_scan('\\acme-hh-mby\c$\data\demo\rolling\process\**\*.parquet',hive_partitioning=1)

create table process_net_conn as select * from parquet_scan('\\acme-hh-mby\c$\data/demo/rolling/process_net_conn/**/*.parquet',hive_partitioning=1)

CREATE TABLE volt AS SELECT * FROM read_csv_auto('\\acme-hh-mby\c$\data/lookups/benignware/class=volt/*.csv',header=true)

CREATE TABLE lolbas AS SELECT * FROM read_csv_auto('\\acme-hh-mby\c$\data/lookups/benignware/class=lolbas/*.csv',header=true)


select hostname, remote_ip_addr, count(distinct pid_hash), count(*) from process_net_conn where remote_port=53  and protocol='TCP' group by all order by hostname

select * from main.process_net_conn 

select date_part('hour',first_seen),
  --trunc(first_seen),
  protocol, count(*)
from main.process_net_conn
where 1=1--	 hostname like 'ACME-DC%'
and dayPK='20231107'
group by all order by all


select p.hostname,p.process_started, min(pnc.first_seen), max(pnc.last_seen), os_pid , p.process_name, args, p.pid_hash, count(*)
from main.process_all p
join main.process_net_conn pnc on p.pid_hash=pnc.pid_hash
where process_name in ('wintap.exe','wintapsvcmgr.exe','sc.exe','net.exe')
--args like '%wintap%' and process_name != 'mergehelper.exe'
group by all order by all 

-- Loose filtering, just process_name
select p.hostname, list(distinct p.user_name), p.process_name, list(distinct daypk), list(distinct p.args), count(distinct p.pid_hash), count(*)
from main.process_all p
join volt b on p.process_name=b.filename 
group by all order by all 

-- Test with args
select p.hostname, list(distinct p.user_name), p.process_name, list(distinct daypk), p.args, b.args, count(distinct p.pid_hash), count(*)
from main.process_all p
join benignware b on p.process_name=b.process_name and levenshtein(p.args,b.args)<20
group by all order by all 

select * from main.process_all where process_name like 'putty%' order by process_started

select daypk, count(*) from process_all where hostname='ACME-HH-EAY' group by all order by all