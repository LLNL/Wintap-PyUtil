-- Recreate PROCESS_SUMMARY with SIGMA, NetworkX labels added. Create a single column for totals to simplify filtering.
create or replace view process_uber_summary
as
select ps.*,
  sl.* exclude (pid_hash),
  ifnull(sl.critical_num_sigma_hits,0)+ifnull(sl.high_num_sigma_hits,0)+ifnull(sl.medium_num_sigma_hits,0)+ifnull(sl.low_num_sigma_hits,0) total_sigma_hits,
  gps.* exclude (pid_hash),
  lbs.* exclude (pid_hash),
  ms.* exclude (pid_hash)
from process_summary ps
left outer join sigma_labels_summary sl on ps.pid_hash=sl.pid_hash
left outer join labels_graph_process_summary gps on ps.pid_hash=gps.pid_hash 
left outer join process_lolbas_summary lbs on ps.pid_hash=lbs.pid_hash
left outer join process_mitre_summary ms on ps.pid_hash=ms.pid_hash
;

