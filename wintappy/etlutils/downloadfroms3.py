"""
Download raw Wintap data from an S3 bucket.

The S3 bucket is expected to be structured as follows:
  [prefix]/raw_sensor/[event_type]/uploadedDPK=YYYYMMDD/uploadedHPK=HH/[hive dirs]/[hostname=event_type-epoch].parquet

The local structure is:
  [localpath]/raw_sensor/[event_type]/dayPK=YYYYMMDD/hourPK=HH/[hostname=event_type-epoch].parquet

Note that the S3 structure is partitioned by day/hour uploaded, while the local path is partitoned by 
the day/hour the data was collected. Difference is notable when the agent has been offline for a period 
of time.
"""

import argparse
import csv
import logging
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

import boto3
import botocore
import tqdm

from wintappy.config import EnvironmentConfig
from wintappy.etlutils.utils import configure_basic_logging, get_date_range

# Maximum number of open HTTP(s) connections
MAX_POOL_CONNECTIONS = 50
# Maximum S3 download threads
MAX_WORKERS = 32
# Maximum number of retries for failed downloads
MAX_RETRIES = 3


@dataclass
class S3File:
    key: str
    filename: str
    s3_path: str
    hostname: str
    data_capture_ts: datetime
    uploadedDPK: str
    uploadedHPK: str
    dataDPK: str
    dataHPK: str
    local_file_path: str
    os: str
    sensor_version: str
    event_type: str

    def dict(self):
        return {k: str(v) for k, v in asdict(self).items()}


def list_files(s3_client, bucket, prefix):
    """
    Lists all files, at any folder level, under the given prefix.
    No folders (CommonPrefixes) are returned as there is no delimiter given.
    """
    return _list_s3(s3_client, bucket, prefix, delimiter="")


def list_folders(s3_client, bucket, prefix):
    """
    Lists all folders (and files) at the given prefix level.
    Note that in practice, the Wintap S3 organization doesn't mix files/folders at the same level.
    """
    return _list_s3(s3_client, bucket, prefix, delimiter="/")


def _list_s3(s3_client, bucket, prefix, delimiter="/"):
    """
    Get files/folders metadata from a specific S3 prefix.
    """
    file_names = []
    folders = []

    kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": delimiter}
    next_token = ""

    while next_token is not None:
        if next_token != "":
            kwargs["ContinuationToken"] = next_token

        response = s3_client.list_objects_v2(**kwargs)
        files = response.get("Contents")
        paths = response.get("CommonPrefixes")
        if paths != None:
            for path in paths:
                folders.append(path)

        if files != None:
            for file in files:
                file_names.append(file)

        next_token = response.get("NextContinuationToken")
    return file_names, folders


def download_one_file(bucket: str, client: boto3.client, s3_file: S3File):
    """
    Download a single file from S3
    Args:
        bucket (str): S3 bucket where images are hosted
        client (boto3.client): S3 client
        s3_file (S3File): S3 object metadata
    """
    make_dirs(s3_file)
    # Replace '=' in filename to avoid DuckDB mistaking it for a key=value pair.
    # Prefix event_type with 'raw_'
    # TODO: This is fixed in Wintap. Still here for legacy data.
    if "=" in s3_file.filename:
        new_filename = s3_file.filename.replace("=", "+raw_")
    else:
        new_filename = s3_file.filename
    client.download_file(
        Bucket=bucket,
        Key=s3_file.key,
        Filename=os.path.join(s3_file.local_file_path, new_filename),
    )


def download_files_threaded(
    bucket: str, client: boto3.client, s3_files, retry_attempt: int = 0
):
    """
    Download files from S3 into the provided root path.
    Files are written to folders based on the timestamp they were collected, not uploaded.
    Multi-threaded, TQDM progress output.
    """

    # The client is shared between threads
    func = partial(download_one_file, bucket, client)

    # List for storing possible failed downloads to retry later
    failed_downloads = []

    with tqdm.tqdm(desc="Downloading files from S3", total=len(s3_files)) as pbar:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
            futures = {executor.submit(func, s3_file): s3_file for s3_file in s3_files}
            for future in as_completed(futures):
                if future.exception():
                    failed_downloads.append(futures[future])
                    logging.error(future.exception())
                pbar.update(1)
    if len(failed_downloads) > 0:
        if retry_attempt < MAX_RETRIES:
            logging.warning(
                f"  {len(failed_downloads)} downloads have failed. Retrying."
            )
            download_files_threaded(bucket, client, failed_downloads, retry_attempt + 1)
        else:
            logging.warning(
                f"  {len(failed_downloads)} downloads have failed. Writing to CSV."
            )
            with open(
                os.path.join(".", f"failed_downloads_{datetime.now()}.csv"),
                "w",
                newline="",
            ) as csvfile:
                wr = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                wr.writerow(failed_downloads)


def download_files(bucket_name, s3_client, s3_files):
    """
    Download files from S3 into the provided root path.
    Files are written to folders based on the timestamp they were collected, not uploaded.
    Single-threaded, simple progress output.
    """
    count = 0
    for s3_file in s3_files:
        make_dirs(s3_file)
        download_one_file(bucket_name, s3_client, s3_file)
        count += 1
        if count % 1000 == 0:
            logging.info(f"      Downloaded: {count}")
    logging.info(f"    Downloaded: {count}")


def make_dirs(s3_file):
    if not os.path.exists(s3_file.local_file_path):
        # When multithreaded, another thread may beat us to creating the path
        os.makedirs(s3_file.local_file_path, exist_ok=True)
        logging.debug("folder '{}' created ".format(s3_file.local_file_path))


def hour_range(start_date, end_date):
    """
    Generate a timestamp for each hour in the range. These will correspond to the paths data is uploaded into.
    """
    for n in range(int((end_date - start_date).total_seconds() / 3600)):
        yield start_date + timedelta(hours=n)


def parse_filename(filename):
    """
    Legacy format: hostname=event_type+epoch_ts.parquet
    New format:    hostname+event_type+epoch_ts.parquet
    """
    if "=" in filename:
        hostname = filename.split("=")[0]
        data_capture_epoch = filename.split("=")[1].rsplit("-")[1].split(".")[0]
    else:
        hostname = filename.split("+")[0]
        # Drop the '.parquet' also
        data_capture_epoch = filename.split("+")[2].split(".")[0]
    return hostname, data_capture_epoch


def parse_s3_metadata(files, dataset, uploadedDPK, uploadedHPK, event_type):
    """
    Parse metadata from S3. This will be used for generating the correct path to write to.
    Writes S3 metadata to a parquet file for metadata analytics.
    TODO: Write S3 metadata into day/hour partitions, the same as other data.
    """
    # Prefix all event_types with "raw_" TODO: fix this in Wintap
    if event_type.lower().startswith("raw_"):
        new_event_type = event_type
    else:
        new_event_type = "raw_" + event_type

    # Modify some event names to what SQL expects
    # To Do: Fix this in Wintap
    if new_event_type == "raw_file":
        new_event_type = "raw_process_file"
    if new_event_type == "raw_registry":
        new_event_type = "raw_process_registry"
    if new_event_type == "raw_processstop":
        # Put process_stop in with process.
        new_event_type = "raw_process"

    files_metadata = []
    back_dated = {}
    for file in files:
        try:
            (s3_path, _, filename) = file.get("Key").rpartition("/")
            hostname, data_capture_epoch = parse_filename(filename)
            data_capture_ts = datetime.fromtimestamp(
                int(data_capture_epoch), timezone.utc
            )
            datadpk = data_capture_ts.strftime("%Y%m%d")
            datahpk = data_capture_ts.strftime("%H")
            # Data date can be different! Thats ok, it just means the host got delayed sending for some reason.
            # TODO: Come up with a "dirty" flag to indicate that backdated data was found so rolling/stdview can be updated
            if datadpk != uploadedDPK or datahpk != uploadedHPK:
                # Key by data date.
                back_dated[(datadpk, datahpk)] = (
                    back_dated.get((datadpk, datahpk), 0) + 1
                )

            # Define fully-qualified local name
            local_file_path = f"{dataset}/raw_sensor/{new_event_type}/dayPK={datadpk}/hourPK={datahpk}"

            s3File = S3File(
                file.get("Key"),
                filename,
                s3_path,
                hostname,
                data_capture_ts,
                uploadedDPK,
                uploadedHPK,
                datadpk,
                datahpk,
                local_file_path,
                "windows",
                "v2",
                event_type,
            )
            files_metadata.append(s3File)
        except Exception as e:
            logging.error(f"Filename parse error on: s3_path: {s3_path} {filename}")
            logging.error(traceback.format_exc())
    if len(back_dated) > 0:
        logging.info(f"Back dated data: {back_dated}")
    return files_metadata


def main(argv=None) -> None:
    configure_basic_logging()
    parser = argparse.ArgumentParser(
        prog="downloadfromS3.py", description="Download Wintap files from S3"
    )
    env_config = EnvironmentConfig(parser)
    env_config.add_aws_settings(required=True)
    env_config.add_start(required=True)
    env_config.add_end(required=True)

    # setup config based on env variables and config file
    args = env_config.get_options(argv)

    if args.AWS_PROFILE:
        session = boto3.Session(profile_name=args.AWS_PROFILE)
    else:
        session = boto3.Session()
    if args.AWS_REGION:
        s3 = session.client(
            "s3",
            config=botocore.client.Config(max_pool_connections=50),
            region_name=args.AWS_REGION,
        )
    else:
        s3 = session.client(
            "s3", config=botocore.client.Config(max_pool_connections=50)
        )

    top_level_prefix = (
        args.AWS_S3_PREFIX
        if args.AWS_S3_PREFIX.endswith("/")
        else f"{args.AWS_S3_PREFIX}/"
    )
    files, folders = list_folders(
        s3, bucket=args.AWS_S3_BUCKET, prefix=top_level_prefix
    )

    # Top level is event types
    event_types = folders
    start_date, end_date = get_date_range(
        args.START, args.END, date_format="%Y%m%d %H", data_set_path=args.DATASET
    )
    if start_date == None and end_date == None:
        # Default to the last day
        end = datetime.now(timezone.utc)
        start_date = datetime(end.year, end.month, end.day) - timedelta(days=1)
        end_date = datetime(end.year, end.month, end.day)

    logging.info(f"Using time range: {start_date} -> {end_date}")
    for event_type in event_types:
        logging.info(f'S3 EventType Prefix: {event_type.get("Prefix")}')
        # Within an event type, iterate over date range by hour
        files_md = []
        for single_date in hour_range(start_date, end_date):
            daypk = single_date.strftime("%Y%m%d")
            hourpk = single_date.strftime("%H")
            logging.debug(f"daypk={daypk}; hourpk={hourpk}")

            # Note: 'Prefix' includes a trailing slash.
            prefix = (
                f"{event_type.get('Prefix')}uploadedDPK={daypk}/uploadedHPK={hourpk}/"
            )

            # Optimization: many event types are sparsely populated, so enumerate the dayPK/hourPK structure, then just get files from the ones that exist.
            files_tmp, existing_S3_paths = list_folders(
                s3,
                bucket=args.AWS_S3_BUCKET,
                prefix=f"{event_type.get('Prefix')}uploadedDPK={daypk}/",
            )
            # list_folders returns a JSON list. Extract the paths as a simple string list
            existing_S3_paths = [x.get("Prefix") for x in existing_S3_paths]
            if prefix in existing_S3_paths:
                files, folders = list_files(
                    s3, bucket=args.AWS_S3_BUCKET, prefix=prefix
                )
                if len(files) > 0 or len(folders) > 0:
                    logging.debug(f"  {prefix}")
                    logging.debug(f"    Files: {len(files)}  Folders: {len(folders)}")
                    files_md.extend(
                        parse_s3_metadata(
                            files,
                            args.DATASET,
                            daypk,
                            hourpk,
                            get_event_type(event_type),
                        )
                    )
                logging.info(
                    f"  {prefix}  Files: {len(files)}  Folders: {len(folders)}  Total: {len(files_md)}"
                )
            else:
                logging.debug(f"  {prefix} not in S3, skipping")

        if len(files_md) > 0:
            logging.info(f"   Downloading {len(files_md)}...")
            download_files_threaded(args.AWS_S3_BUCKET, s3, files_md)

        # Write metadata
        # Ugly conversion to list of dicts to be able to easily create parquet.
        tmp_files = []
        for file in files_md:
            tmp_files.append(file.dict())
        s3_table = pa.Table.from_pylist(tmp_files)
        Path(f"{args.DATASET}/s3_metadata").mkdir(parents=True, exist_ok=True)
        pq.write_table(
            s3_table,
            f'{args.DATASET}/s3_metadata/s3_metadata-{get_event_type(event_type)}-{args.START.replace(" ","_")}-{args.END.replace(" ","_")}.parquet',
        )


def get_event_type(event_type):
    return event_type.get("Prefix").split("/")[2]


if __name__ == "__main__":
    main(argv=None)
