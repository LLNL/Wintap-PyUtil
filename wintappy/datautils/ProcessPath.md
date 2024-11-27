For larger datasets, processing by partitioning is required. 

Pseudo code:

setup
   create table tmp_process where false (just need object, no data, at this point)
   create view process_path_v1
   create table process_path as from process_path_v1 where false 

for host in hosts:
   drop table tmp_process
   create table tmp_process where hostname=host
   insert into process_path select * from process_path_v1

Single host tests (ACME-Redo: ACME-DC1)

No index: Run Time (s): real 516.086 user 996.254755 sys 374.332785
Pid_Hash: Run Time (s): real 523.609 user 1014.446506 sys 396.068710
ParentPH: Run Time (s): real 524.081 user 1015.510835 sys 392.339936

Remove p.hostname=pt.hostname qualifier
ParentPH: Run Time (s): real 515.749 user 974.264660 sys 382.148094