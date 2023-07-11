"""
Functions loading data from the standard view structure
"""

import os
import re
from collections import defaultdict
from datetime import datetime

import altair as alt
import pandas as pd
from dotenv import load_dotenv
from humanfriendly import format_size
from IPython.display import Markdown, display


def load_files_from_dataset(dataset_path):
    """
    Load all files from a structure data set into a dictionary keyed by event.
    The result are discrete, fully qualified filenames by event type with no globs.
    Expected structure is: {dataset}/raw/{eventType}/{attr=value}/../{filename}.parquet
    """
    event_types = os.listdir(dataset_path)
    batch = defaultdict(list)
    for event_type in event_types:
        if os.path.isdir(dataset_path + "/" + event_type):
            # Structured format: subdir(s), multiple parquet files supported.
            for dirpath, _, files in os.walk(f"{dataset_path}/{event_type}", followlinks=True):
                if len(files) > 0:
                    print(f"{datetime.now()} Listing event files: {dirpath}")
                    for file in files:
                        if file.lower().endswith("parquet"):
                            batch[event_type].append(dirpath + "/" + file)
        else:
            # Treat as a simple, single file.
            if event_type.lower().endswith("parquet"):
                event = re.split(r"\.", event_type)[0]
                print(f"{datetime.now()} Loading {event} file: {event_type}")
                batch[event].append(dataset_path + "/" + event_type)
    return batch


def load_data(batch):
    """
    Load files from batch into pandas dataframes. No processing done here.
    Note: this does implicitly merge multiple source files into a single panda dataframe.
    """
    print(f"{datetime.now()}  Loading data into pandas")
    batchdf = {}
    for event_type, files in batch.items():
        batchdf[f"{event_type}"] = pd.read_parquet(files, engine="pyarrow")
    return batchdf


class WintapDataset:
    def __init__(self, dataset_path, agg_level="stdview"):
        self.dataset_path = dataset_path
        self.agg_level = agg_level

        self.files = load_files_from_dataset(dataset_path + "/" + agg_level)
        self.pandasdf = load_data(self.files)

    def __str__(self):
        return f"{self.agg_level} in {self.dataset_path} from: fixme to: fixme"

    def __repr__(self):
        return f"WintapDS({self.dataset_path},{self.agg_level})"

    @property
    def process(self):
        return self.pandasdf["process"]

    @property
    def process_file(self):
        return self.pandasdf["process_file"]

    @property
    def process_conn_incr(self):
        return self.pandasdf["process_conn_incr"]

    @property
    def process_net_conn(self):
        return self.pandasdf["process_net_conn"]

    @property
    def process_net_summary(self):
        return self.pandasdf["process_net_summary"]

    @property
    def process_registry(self):
        return self.pandasdf["process_registry"]

    @property
    def process_events(self):
        return self.pandasdf["process_events"]

    def search_process_name(self, term):
        return self.process.loc[
            (self.process["process_name"].notnull())
            & (self.process["process_name"].str.startswith(term))
        ]

    def search_process_name_in(self, term_list):
        found = pd.DataFrame()
        for term in term_list:
            found = pd.concat([found, self.search_process_name(term)])

        return found


######## Various dataset display widgets for Jupyter


def data_summary(pandasdf):
    """
    Display a short summary of a set of pandas
    """
    # Note: Markdown only seems to work when given the entire table. Fails if you try to use display(Markdown(a row)) iteratively
    table = "| EventType | Rows | Memory\n"
    table += "| :- | -: | -: |\n"

    for event_type in sorted(pandasdf):
        table += f"| {event_type} | {pandasdf[event_type].shape[0]} | {format_size(pandasdf[event_type].memory_usage(index=True).sum())} \n"

    display(Markdown(table))


def calc_event_summary(eventdf, dtfield, event_type):
    # Truncate to minute for binning
    # TODO: ability to provide bin size: 15 min, 2 hours, etc.
    eventdf["BinDT"] = eventdf[dtfield].dt.floor("min")
    if "event_count" in eventdf.columns:
        # Events are aggregated in sensor, so sum them.
        col = "event_count"
        func = "sum"
    else:
        # Each raw row is an event, just count them.
        col = "pid_hash"
        func = "count"

    # Aggregate into bins
    eventdf = eventdf.groupby(["hostname", "BinDT"], dropna=False, as_index=False).agg(
        NumRows=pd.NamedAgg(column=col, aggfunc=func)
    )
    eventdf["Event"] = event_type
    # Calcuate "robust" scaling
    eventdf["NumRowsRobust"] = (eventdf["NumRows"] - eventdf["NumRows"].median()) / (
        eventdf["NumRows"].quantile(0.75) - eventdf["NumRows"].quantile(0.25)
    )
    # Max size for circle marker should be ~600. Calculate multiplier to use based on max robust value.
    size_mx = 600 / eventdf["NumRowsRobust"].max()
    # Hmm, need positive values for a sensible marker size. Shift'em. Note: min is assumed to always be < 0.
    eventdf["NumRowsRobust"] = (
        eventdf["NumRowsRobust"] + eventdf["NumRowsRobust"].min() * -1 + 0.1
    ) * size_mx
    # display(eventDF.sort_values('NumRowsRobust'))
    return eventdf


def events_chart_description():
    """
    Return the description for the events chart as markdown.
    """
    display(
        Markdown(
            """
## Overview Chart of Host Events Over Time.
 Intended for small host sets (1-?) and short time ranges (5min - ? hours?).

### Reading the chart:

* Event counts are aggregated in minute bins. Counts are ETW events.
* Event totals are normalized within type to support generating reasonable sized markers
    * Min/max are targeted to fit nicely in the fixed size Y band per host/event type.
* Time range is from the earliest to latest files from Wintap.
    * Note: Process event time range will include processes BEFORE the start of files. This is due to Wintap reading in process info back to boot of the system.   
    """
        )
    )


def show_events_chart(pandasdf, display_description=True):
    if display_description:
        events_chart_description()

    # Define how to aggregate Pandas
    events = [
        ("process", "process_started", "proc"),
        ("process_image_load", "first_seen", "dll"),
        ("process_registry", "first_seen", "reg"),
        ("process_file", "first_seen", "file"),
        ("process_conn_incr", "first_seen", "net"),
    ]

    events_calc = [
        calc_event_summary(pandasdf[event[0]], event[1], event[2])
        for event in events
        if event[0] in pandasdf
    ]
    all_events = pd.concat(events_calc)

    # Create a compound key for the Y. Can't seem to specify it in the altair syntax
    all_events["y"] = all_events["hostname"] + ": " + all_events["Event"]

    plot_obj = (
        alt.Chart(all_events)
        .mark_circle()
        .encode(
            x="BinDT",
            y="y",
            #    y='Hostname',
            size=alt.Size("NumRowsRobust:N", scale=None),
            color="Event",
            tooltip=["Event:N", "NumRows:Q"],
        )
        .properties(width=1200, height=400, title="Raw Events over Time")
        .interactive()
    )
    display(plot_obj)
