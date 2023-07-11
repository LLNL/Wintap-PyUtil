import logging
import os
from glob import iglob
from typing import NamedTuple

import altair as alt
import pandas as pd
from humanfriendly import format_size
from jinja2 import Template

from wintappy.datautils import rawutil as ru

EventSummaryColumn = NamedTuple(
    "EventSummaryColumn",
    [
        ("table", str),
        ("label", str),
        ("host_col", str),
        ("ts_func", str),
        ("num_event_func", str),
    ],
)


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
    return esm


def create_event_summary_view(con):
    """
    Create a view for all known raw event types.
    To add a new type, define in the event_summary_metadata.
    """
    esms = event_summary_metadata()
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
    SELECT '{{esm.label}}' as Event,
    {{esm.host_col}} as Hostname,
    {{esm.ts_func}} bin_date,
    {{esm.num_event_func}} NumRows
    FROM {{esm.table}}
    GROUP BY ALL
    {% if not loop.last %}UNION{% endif %}
    {%- endfor %}
    """

    sql = Template(template).render(esms=tables)
    logging.debug(f"Generated summary view: {sql}")
    con.execute(sql)


def init_db(con, bucket_size="30 minutes"):
    # Create a macro (function) that will create the time bins.
    # To do: derive the intertotal_size based on the dataset time range and the desired target number of data points. The data points size directly affects performance of the chart. Too fine-grained isn't generally useful.
    con.execute(
        f"create or replace macro tb(wts) as time_bucket(interval '{bucket_size}', to_timestamp_micros(win32_to_epoch(wts)))"
    )
    create_event_summary_view(con)


def duckdb_table_metadata(con):
    # Ignore objects ending in _v1 as they are likely complex view and can be expensive to count.
    tablesDF = con.execute(
        "select table_name from information_schema.tables where table_name not like '%_v1' order by all"
    ).df()
    tables = tablesDF["table_name"].tolist()
    template = """
    {%- for table in tables %}
    SELECT '{{table}}' as Table_Name, min(daypk) Min_DayPK, max(daypk) Max_DayPK, count(*) as Num_Rows
    FROM {{table}}
    {% if not loop.last %}UNION{% endif %}
    {%- endfor %}
    ORDER BY table_name
    """

    sql = Template(template).render(tables=tables)
    logging.debug(f"Generated sql: {sql}")
    return con.execute(sql).df()


def table_summary(con, dataset, agg_level="rolling"):
    """
    Get the list of tables defined in duckdb, then add sizes for the associated parquet files.
    """
    tablesDF = duckdb_table_metadata(con)

    globs = ru.get_glob_paths_for_dataset(dataset, agg_level)
    for event, glob in globs.items():
        cur_size = cur_files = 0
        for file in iglob(glob):
            cur_size += os.path.getsize(file)
            cur_files += 1
        tablesDF.loc[tablesDF.Table_Name == event, "Size"] = format_size(cur_size)
        tablesDF.loc[tablesDF.Table_Name == event, "Files"] = cur_files
    return tablesDF


def fetch_summary_data(con):
    eventDF = con.execute(
        'select "Event", "Hostname",bin_date as BinDT,"NumRows" from event_summary_raw_v1 order by all'
    ).df()

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


def display_event_chart(eventDF):
    # Set jupyter options
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_colwidth", None)

    # Set altair options
    alt.data_transformers.disable_max_rows()

    allEvents = eventDF

    # Create a compound key for the Y. Can't seem to specify it in the altair syntax
    allEvents["Hostname+Event"] = allEvents["Hostname"] + ": " + allEvents["Event"]

    eventsChart = (
        alt.Chart(allEvents)
        .mark_circle()
        .encode(
            x="BinDT",
            y="Hostname+Event",
            # size=alt.Size('NumRowsRobust:N', scale=None),
            # size=20,
            color="Event",
            tooltip=["Hostname:N", "Event:N", "NumRows:Q", "BinDT"],
        )
        .properties(width=1200, height=600, title="Raw Events over Time")
        .interactive()
    )
    display(eventsChart)
