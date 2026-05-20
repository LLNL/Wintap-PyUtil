select
    agentid agent_id,
    hostname,
    any_value('windows') os_family,
    case when privategateway = '' then null else privategateway end private_gateway,
    {{ int_to_ip_expr('cast(ipaddr as bigint)') }} ip_addr_no,
    case when mac = '' then null else mac end mac,
    ipaddr ip_addr,
    'missing?' interface,
    mtu,
    to_timestamp(min(cast(eventtime as bigint))) first_seen,
    to_timestamp(max(cast(eventtime as bigint))) last_seen,
    count(*) num_rows
from {{ ref('stg_raw_macip') }}
group by all
