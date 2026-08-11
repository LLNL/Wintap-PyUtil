import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full")


@app.cell
def _():
    import os
    from pathlib import Path

    import altair as alt
    import duckdb
    import marimo as mo
    import pandas as pd

    def db_path() -> str:
        if os.getenv("WINTAP_DBT_DATABASE"):
            return os.environ["WINTAP_DBT_DATABASE"]
        data_root = os.getenv("WINTAP_DATA_ROOT")
        if data_root:
            return str(Path(data_root) / "duckdb" / "wintap.duckdb")
        return "wintap.duckdb"

    def query_df(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
        try:
            return con.execute(sql).df()
        except Exception as exc:  # Keep dashboard usable when optional models are absent.
            return pd.DataFrame({"error": [str(exc)], "sql": [sql]})

    def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        return bool(
            con.execute(
                """
                select count(*)
                from information_schema.tables
                where table_schema = 'main'
                  and lower(table_name) = lower(?)
                """,
                [table_name],
            ).fetchone()[0]
        )

    return Path, alt, db_path, duckdb, mo, query_df, table_exists


@app.cell
def _(mo):
    mo.md("""
    # Wintap/Lintap DBT QA Dashboard

    Quick overview for a DBT-built Wintap/Lintap DuckDB database. The database path is resolved from
    `WINTAP_DBT_DATABASE`, or from `WINTAP_DATA_ROOT/duckdb/wintap.duckdb` when only `WINTAP_DATA_ROOT` is set.
    """)
    return


@app.cell
def _(Path, db_path, mo):
    database_path = db_path()
    database_exists = Path(database_path).exists()
    mo.vstack(
        [
            mo.md(f"**Database:** `{database_path}`"),
            mo.md(f"**Exists:** `{database_exists}`"),
        ]
    )
    return database_exists, database_path


@app.cell
def _(database_exists, database_path, duckdb, mo):
    if not database_exists:
        mo.stop(True, mo.md("Database not found. Run `make dbt-build` first."))
    con = duckdb.connect(database_path, read_only=True)
    return (con,)


@app.cell
def _(con, mo, query_df):
    build_summary = query_df(con, "select * from build_summary order by table_name")
    event_summary = query_df(con, "select * from telemetry_event_summary order by event_type")
    mo.md("## Build and Event Summary")
    return build_summary, event_summary


@app.cell
def _(build_summary, event_summary, mo):
    mo.vstack(
        [
            mo.md("### DBT build row counts"),
            mo.ui.table(build_summary, pagination=False),
            mo.md("### Telemetry event summary"),
            mo.ui.table(event_summary, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df):
    qa_pid_hash = query_df(
        con,
        """
        with orphan_checks as (
            select 'process_conn_incr' as child_relation, count(*) as child_rows,
                   count(p.pid_hash) as matched_rows,
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
                   count(*) filter (where c.pid_hash is null) as null_pid_hash_rows
            from process_conn_incr c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_net_conn', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_net_conn c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_file', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_file c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_registry', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_registry c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_image_load', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_image_load c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_summary', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_summary c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_uber_summary', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_uber_summary c left join process p on c.pid_hash = p.pid_hash
        )
        select *,
               case when orphan_rows = 0 and null_pid_hash_rows = 0 then 'PASS' else 'FAIL' end as status
        from orphan_checks
        order by child_relation
        """,
    )
    sentinel_pid_hash = query_df(
        con,
        """
        select 'process' as relation_name, pid_hash, count(*) as num_rows
        from process
        where lower(pid_hash) like 'fixme%' or lower(pid_hash) like 'unknown%'
        group by all
        union all
        select 'process_conn_incr', pid_hash, count(*)
        from process_conn_incr
        where lower(pid_hash) like 'fixme%' or lower(pid_hash) like 'unknown%'
        group by all
        union all
        select 'process_file', pid_hash, count(*)
        from process_file
        where lower(pid_hash) like 'fixme%' or lower(pid_hash) like 'unknown%'
        group by all
        order by relation_name, num_rows desc
        """,
    )
    mo.md("## PID Hash QA")
    return qa_pid_hash, sentinel_pid_hash


@app.cell
def _(mo, qa_pid_hash, sentinel_pid_hash):
    mo.vstack(
        [
            mo.md("### PID hash orphan checks"),
            mo.ui.table(qa_pid_hash, pagination=False),
            mo.md("### Forbidden/sentinel PID hash values"),
            mo.ui.table(sentinel_pid_hash, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df):
    host_user_summary = query_df(
        con,
        """
        select
            count(distinct hostname) as num_hosts,
            count(distinct agent_id) as num_agent_ids,
            count(distinct user_name) filter (where user_name is not null) as num_users,
            count(distinct process_name) as num_process_names,
            count(distinct os_pid) as num_os_pids,
            count(*) as num_process_instances
        from process
        """,
    )
    top_hosts = query_df(
        con,
        """
        select hostname, count(*) as process_instances, count(distinct user_name) as users,
               count(distinct process_name) as process_names
        from process
        group by hostname
        order by process_instances desc
        limit 25
        """,
    )
    top_users = query_df(
        con,
        """
        select user_name, count(*) as process_instances, count(distinct hostname) as hosts,
               count(distinct process_name) as process_names
        from process
        where user_name is not null
        group by user_name
        order by process_instances desc
        limit 25
        """,
    )
    mo.md("## Hosts, Users, Processes")
    return host_user_summary, top_hosts, top_users


@app.cell
def _(host_user_summary, mo, top_hosts, top_users):
    mo.vstack(
        [
            mo.ui.table(host_user_summary, pagination=False),
            mo.md("### Top hosts"),
            mo.ui.table(top_hosts, pagination=False),
            mo.md("### Top users"),
            mo.ui.table(top_users, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df):
    timeline = query_df(
        con,
        """
        select time_chunk, event_type, num_rows
        from process_chart
        union all
        select time_chunk, event_type, num_rows from file_chart
        union all
        select time_chunk, event_type, num_rows from network_chart
        union all
        select time_chunk, event_type, num_rows from perf_chart
        order by time_chunk, event_type
        """,
    )
    mo.md("## Timeline")
    return (timeline,)


@app.cell
def _(alt, mo, timeline):
    if timeline.empty or "error" in timeline.columns:
        chart = mo.ui.table(timeline, pagination=False)
    else:
        chart = alt.Chart(timeline).mark_bar().encode(
            x=alt.X("time_chunk:T", title="Time"),
            y=alt.Y("num_rows:Q", title="Rows / samples"),
            color="event_type:N",
            tooltip=["time_chunk:T", "event_type:N", "num_rows:Q"],
        ).properties(height=320)
    chart
    return


@app.cell
def _(con, mo, query_df):
    top_processes = query_df(
        con,
        """
        select process_name, count(*) as process_instances, count(distinct hostname) as hosts,
               count(distinct user_name) as users
        from process
        group by process_name
        order by process_instances desc
        limit 30
        """,
    )
    top_network = query_df(
        con,
        """
        select remote_ip_addr, remote_port, protocol, count(*) as connections,
               sum(total_events) as total_events, sum(total_size) as total_size
        from process_net_conn
        where remote_ip_addr is not null
        group by all
        order by total_events desc nulls last, connections desc
        limit 30
        """,
    )
    top_files = query_df(
        con,
        """
        select filename, count(*) as process_file_rows, sum(event_count) as events,
               sum(bytes_requested) as bytes_requested
        from process_file
        group by filename
        order by events desc nulls last, bytes_requested desc nulls last
        limit 30
        """,
    )
    mo.md("## Top Activity")
    return top_files, top_network, top_processes


@app.cell
def _(mo, top_files, top_network, top_processes):
    mo.vstack(
        [
            mo.md("### Top process names"),
            mo.ui.table(top_processes, pagination=False),
            mo.md("### Top network endpoints"),
            mo.ui.table(top_network, pagination=False),
            mo.md("### Top files"),
            mo.ui.table(top_files, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df, table_exists):
    pidstat_available = table_exists(con, "pidstat_metrics")
    pidstat_summary = query_df(
        con,
        """
        select
            count(*) as samples,
            min(time) as first_seen,
            max(time) as last_seen,
            max(cpu_percent) as max_cpu_percent,
            max(mem_percent) as max_mem_percent,
            max(kb_read_per_sec) as max_kb_read_per_sec,
            max(kb_write_per_sec) as max_kb_write_per_sec,
            count(distinct pid) as num_pids,
            count(distinct command) as num_commands
        from pidstat_metrics
        """,
    )
    top_pidstat = query_df(
        con,
        """
        select command, pid,
               max(cpu_percent) as max_cpu_percent,
               max(mem_percent) as max_mem_percent,
               max(kb_read_per_sec) as max_kb_read_per_sec,
               max(kb_write_per_sec) as max_kb_write_per_sec,
               count(*) as samples
        from pidstat_metrics
        group by command, pid
        order by max_cpu_percent desc nulls last, max_mem_percent desc nulls last
        limit 30
        """,
    )
    mo.md("## Pidstat")
    return pidstat_available, pidstat_summary, top_pidstat


@app.cell
def _(mo, pidstat_available, pidstat_summary, top_pidstat):
    mo.vstack(
        [
            mo.md(f"**pidstat_metrics model exists:** `{pidstat_available}`"),
            mo.ui.table(pidstat_summary, pagination=False),
            mo.md("### Top pidstat processes"),
            mo.ui.table(top_pidstat, pagination=False),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
