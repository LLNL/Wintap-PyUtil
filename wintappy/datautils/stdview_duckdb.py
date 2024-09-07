import logging
import os
from dataclasses import dataclass
from glob import iglob
from typing import NamedTuple

import altair as alt
import pandas as pd
from humanfriendly import format_size
from jinja2 import Template

from wintappy.datautils import rawutil as ru


@dataclass
class EventSummaryColumn:
    table: str
    label: str
    host_col: str
    ts_func: str
    num_event_func: str


def event_summary_metadata():
    """
    Define metadata for known tables that is used to generate SQL for the overall summary.
    """
    esm = []
    # esm.append(EventSummaryColumn('raw_host','host','Hostname','tb2(EventTime)', 'count(*)'))
    esm.append(
        EventSummaryColumn(
            "raw_process", "process", "Hostname", "tb(EventTime)", "count(*)"
        )
    )
    esm.append(
        EventSummaryColumn(
            "raw_process_conn_incr",
            "network",
            "Hostname",
            "tb(cast(firstseenms as bigint))",
            "sum(eventcount)",
        )
    )
    esm.append(
        EventSummaryColumn(
            "raw_process_file",
            "file",
            "Hostname",
            "tb(cast(firstseen as bigint))",
            "sum(eventcount)",
        )
    )
    esm.append(
        EventSummaryColumn(
            "raw_imageload", "dll", "ComputerName", "tb(EventTime)", "count(*)"
        )
    )
    esm.append(
        EventSummaryColumn(
            "raw_process_registry",
            "registry",
            "HostHame",
            "tb(FirstSeenMs)",
            "sum(eventcount)",
        )
    )
    esm.append(
        EventSummaryColumn(
            "raw_genericmessage",
            "generic_message",
            "ComputerName",
            "tb(EventTime)",
            "count(*)",
        )
    )
    esm.append(
        EventSummaryColumn(
            "raw_kernelapicall",
            "kernelapicall",
            "ComputerName",
            "tb(EventTime)",
            "count(*)",
        )
    )
    return esm


def create_event_summary_view(con, min_daypk, max_daypk):
    """
    Create a view for all known raw event types.
    To add a new type, define in the event_summary_metadata.
    """
    esms = event_summary_metadata()
    print(esms)
    tablesDF = con.execute(
        "select table_name from information_schema.tables where table_name like 'raw_%'"
    ).df()
    db_tables = tablesDF["table_name"].tolist()

    tables = [esm for esm in esms if esm.table in db_tables]
    logging.debug(f"Found: {tables}")
    logging.info(f"Missing: {set(db_tables) - set(esm.table for esm in esms)}")

    template = """
    CREATE OR replace VIEW event_summary_raw_v1
    AS
    {%- for esm in esms %}
    SELECT
        '{{esm.label}}' as Event,
        upper({{esm.host_col}}) as Hostname,
        agentid,
        {{esm.ts_func}} bin_date,
        {{esm.num_event_func}} NumRows
    FROM {{esm.table}}
    WHERE dayPK between {{min_daypk}} and {{max_daypk}}
    GROUP BY ALL
    {% if not loop.last %}UNION{% endif %}
    {%- endfor %}
    """

    sql = Template(template).render(
        esms=tables, min_daypk=min_daypk, max_daypk=max_daypk
    )
    logging.debug(f"Generated summary view: {sql}")
    print(sql)
    con.execute(sql)


def calc_time_bucket(min_dayPK, max_dayPK, num_buckets=500):
    """
    Calculate time bucket size (as an interval).
    """
    return str((max_dayPK - min_dayPK) / num_buckets)


def init_db(con, min_dayPK, max_dayPK, bucket_size=None):
    # Create a macro (function) that will create the time bins.

    # If not provided, calc the default size based on the time range.
    if not bucket_size:
        bucket_size = calc_time_bucket(min_dayPK, max_dayPK)
    print(bucket_size)

    # Once min/max are calc'd here, they can be passed into creating the view
    con.execute(
        f"create or replace macro tb(wts) as time_bucket(interval '{bucket_size}', cast(to_timestamp_micros(win32_to_epoch(wts)) as timestamp))"
    )
    # Convert from Timestamps to int
    create_event_summary_view(
        con, int(min_dayPK.strftime("%Y%m%d")), int(max_dayPK.strftime("%Y%m%d"))
    )


def duckdb_table_metadata(con, include_paritioned_data=True):
    # Ignore objects ending in _v1 as they are likely complex view and can be expensive to count.
    in_clause = "IN" if include_paritioned_data else "NOT IN"
    tablesDF = con.execute(
        f"select table_name from information_schema.tables where table_name not like '%_v1' and table_name {in_clause} ( select table_name from information_schema.columns WHERE column_name = 'dayPK' ) order by all"
    ).df()
    tables = tablesDF["table_name"].tolist()
    if include_paritioned_data:
        template = """
        {%- for table in tables %}
        SELECT '{{table}}' as Table_Name, min(daypk) Min_DayPK, max(daypk) Max_DayPK, count(*) as Num_Rows
        FROM {{table}}
        {% if not loop.last %}UNION ALL{% endif %}
        {%- endfor %}
        ORDER BY table_name
        """
    else:
        template = """
        {%- for table in tables %}
        SELECT '{{table}}' as Table_Name, count(*) as Num_Rows
        FROM {{table}}
        {% if not loop.last %}UNION ALL{% endif %}
        {%- endfor %}
        ORDER BY table_name
        """

    if not tablesDF.empty:
        sql = Template(template).render(tables=tables)
        logging.debug(f"Generated sql: {sql}")
        return con.execute(sql).df()
    else:
        return tablesDF


def table_summary(
    con, dataset, lookups="", agg_level="rolling", include_paritioned_data=True
):
    """
    Get the list of tables defined in duckdb, then add sizes for the associated parquet files.
    """
    tablesDF = duckdb_table_metadata(con, include_paritioned_data)
    if not tablesDF.empty:
        globs = ru.get_glob_paths_for_dataset(dataset, agg_level, lookups=lookups)
        for event, glob in globs.items():
            cur_size = cur_files = 0
            for file in iglob(glob):
                cur_size += os.path.getsize(file)
                cur_files += 1
            tablesDF.loc[tablesDF.Table_Name == event, "Size"] = format_size(cur_size)
            tablesDF.loc[tablesDF.Table_Name == event, "Files"] = cur_files
    return tablesDF


def fetch_summary_data(con, hostname="%", agent_id="%"):
    # To get mixed-case column names in the DF, use "". But to use strings in the WHERE, use ''.
    sql = (
        'select "Event", "Hostname",bin_date as BinDT,"NumRows" from event_summary_raw_v1'
        + f" where hostname ilike '%{hostname}%' and agentid ilike '%{agent_id}%' order by all"
    )
    print(sql)
    eventDF = con.execute(sql).df()

    # Calcuate "robust" scaling. eventDF has ALL event types so, need to handle that:
    for event in eventDF["Event"].unique():
        logging.debug(f"Event: {event}")
        eventDF.loc[eventDF.Event == event, "NumRowsRobust"] = (
            eventDF["NumRows"] - eventDF["NumRows"].median()
        ) / (eventDF["NumRows"].quantile(0.75) - eventDF["NumRows"].quantile(0.25))
        # Max size for circle marker should be ~600. Calculate multiplier to use based on max robust total_sizeue.
        sizeMx = 600 / eventDF.loc[eventDF.Event == event, "NumRowsRobust"].max()
        logging.debug(f"Min: {eventDF.loc[eventDF.Event==event,'NumRowsRobust'].min()}")
        logging.debug(
            f"Med: {eventDF.loc[eventDF.Event==event,'NumRowsRobust'].median()}"
        )
        logging.debug(f"Max: {eventDF.loc[eventDF.Event==event,'NumRowsRobust'].max()}")
        # Hmm, need positive total_size for a sensible marker size. Shift'em. Note: min is assumed to always be < 0.
        eventDF.loc[eventDF.Event == event, "NumRowsRobust"] = (
            eventDF["NumRowsRobust"] + eventDF["NumRowsRobust"].min() * -1 + 0.5
        ) * sizeMx
    return eventDF


def create_event_chart(eventDF):
    # Set altair options
    alt.data_transformers.disable_max_rows()

    # Create a compound key for the Y. Can't seem to specify it in the altair syntax
    allEvents = eventDF.assign(Hostname_Event=lambda x: x.Hostname + ": " + x.Event)

    eventsChart = (
        alt.Chart(allEvents)
        .mark_circle()
        .encode(
            alt.X("BinDT:T"),
            alt.Y("Hostname_Event:N"),
            # size=alt.Size('NumRowsRobust:N', scale=None),
            alt.Size(
                "NumRows:Q",
                scale=alt.Scale(range=[0, 4000]),
                legend=alt.Legend(title="Events per bucket"),
            ),
            # size=20,
            color="Event:N",
            tooltip=[
                "Hostname:N",
                "Event:N",
                "NumRows:Q",
                alt.Tooltip("BinDT:T", format="%Y-%m-%d %H:%M:%S"),
            ],
        )
        .properties(title="Raw Events over Time")
        .interactive()
    )
    return eventsChart


def display_event_chart(eventDF, width=1200, height=600):
    # Set jupyter options
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", None)

    display(create_event_chart(eventDF).properties(width=width, height=height))


def create_data_summary(datasummaryDF):
    # Create line charts for many features. Each chart will then have its own Y-Scale
    # Set altair options
    alt.data_transformers.disable_max_rows()

    hostChart = (
        alt.Chart(datasummaryDF)
        .mark_line(interpolate="monotone")
        .encode(
            alt.X("bucket_day"),
            alt.Y("hosts"),
            tooltip=["bucket_day"],
        )
    )
    uniqProcessChart = (
        alt.Chart(datasummaryDF)
        .mark_line(interpolate="monotone", stroke="grey")
        .encode(
            alt.X("bucket_day"),
            alt.Y("uniq_processes"),
            tooltip=["bucket_day"],
        )
    )

    return (hostChart + uniqProcessChart).resolve_scale(y="independent").interactive()
