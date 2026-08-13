select
    ps.*,
    sl.* exclude (pid_hash),
    ifnull(sl.critical_num_sigma_hits, 0) + ifnull(sl.high_num_sigma_hits, 0) + ifnull(sl.medium_num_sigma_hits, 0) + ifnull(sl.low_num_sigma_hits, 0) total_sigma_hits,
    gps.* exclude (pid_hash),
    lbs.* exclude (pid_hash),
    ms.* exclude (pid_hash),
    if(lower(ps.user_name) like '%bad%', 'BAD', null) bad_user,
    if(ifnull(gps.label_num_sources, 0) > 0 or bad_user is not null, 1, 0) red_team
from {{ ref('process_summary') }} ps
left outer join {{ ref('sigma_labels_summary') }} sl on ps.pid_hash = sl.pid_hash
left outer join {{ ref('labels_graph_process_summary') }} gps on ps.pid_hash = gps.pid_hash
left outer join {{ ref('process_lolbas_summary') }} lbs on ps.pid_hash = lbs.pid_hash
left outer join {{ ref('process_mitre_summary') }} ms on ps.pid_hash = ms.pid_hash
