from datetime import datetime, timedelta, timezone
import os
import argparse
import sys
import boto3

import logging
from typing import NamedTuple

S3File = NamedTuple(
    "S3File",
    [
        ("filename", str),
        ("s3_path", str),
        ("hostname", str),
        ("data_capture_ts", datetime),
        ("uploadedDPK", str),
        ("uploadedHPK", str),
        ("dataDPK", str),
        ("dataHPK", str),
        ("os", str),
        ("sensor_version", str),
        ("event_type", str),
    ],
)


def list_files(s3_client, bucket, prefix):
    """
    Lists all files, at any folder level, under the given prefix.
    No folders (CommonPrefixes) are returned as there is no delimiter given.
    """
    return _list_s3(s3_client, bucket, prefix, delimiter="")


def list_folders(s3_client, bucket, prefix):
    """
    Lists all folders (and files) at the given prefix level.
    Note that in practice, the wintap S3 organization doesn't mix files/folders at the same level.
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


def download_files(s3_client, bucket_name, local_path, s3_files):
    """
    Download files from S3 into the provided root path.
    Files are written to folders based on the timestamp they were collected, not uploaded.
    """
    count = 0
    for s3_file in s3_files:
        s3_file_path = s3_file.s3_path + "/" + s3_file.filename
        local_file_path = f"{local_path}/raw_sensor/{s3_file.event_type}/dayPartitionKey={s3_file.dataDPK}/hourPartitionKey={s3_file.dataHPK}"
        if not os.path.exists(local_file_path):
            os.makedirs(local_file_path)
            logging.info("folder '{}' created ".format(local_file_path))
        s3_client.download_file(
            bucket_name, s3_file_path, local_file_path + "/" + s3_file.filename
        )
        count += 1
        if count % 1000 == 0:
            logging.info(f"      Downloaded: {count}")
    logging.info(f"    Downloaded: {count}")


def hourrange(start_date, end_date):
    """
    Generate a timestamp for each hour in the range. These will correspond to the paths data is uploaded into.
    """
    for n in range(int((end_date - start_date).total_seconds() / 3600)):
        yield start_date + timedelta(hours=n)


def parse_s3_metadata(files, uploadedDPK, uploadedHPK, event_type):
    """
    Parse metadata from S3. This will be used for generating the correct path to write to.
    TODO: Write this data also to a parquet file for metadata analytics.
    """
    files_metadata = []
    for file in files:
        (s3_path, delim, filename) = file.get("Key").rpartition("/")
        hostname = filename.split("=")[0]
        data_capture_epoch = filename.split("=")[1].rsplit("-")[1].split(".")[0]
        data_capture_ts = datetime.fromtimestamp(int(data_capture_epoch), timezone.utc)
        datadpk = data_capture_ts.strftime("%Y%m%d")
        datahpk = data_capture_ts.strftime("%H")

        s3File = S3File(
            filename,
            s3_path,
            hostname,
            data_capture_ts,
            uploadedDPK,
            uploadedHPK,
            datadpk,
            datahpk,
            "windows",
            "v2",
            event_type,
        )
        files_metadata.append(s3File)
    return files_metadata


def main():
    parser = argparse.ArgumentParser(
        prog="downloadFromS3.py", description="Download Wintap files from S3"
    )
    parser.add_argument("--profile", help="AWS profile to use", required=True)
    parser.add_argument("-b", "--bucket", help="The S3 bucket", required=True)
    parser.add_argument("-p", "--prefix", help="S3 prefix within the bucket", required=True)
    parser.add_argument("-s", "--start", help="Start date (MM/DD/YYYY HH)", required=True)
    parser.add_argument("-e", "--end", help="End date (MM/DD/YYYY HH)", required=True)
    parser.add_argument("-l", "--localpath", help="Local path to write files", required=True)
    parser.add_argument('--log-level', default='INFO', help='Logging Level: INFO, WARN, ERROR, DEBUG')
    args = parser.parse_args()

    try:
        logging.basicConfig(level=args.log_level,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    except ValueError:
        logging.error('Invalid log level: {}'.format(args.log_level))
        sys.exit(1)

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")

    files, folders = list_folders(s3, bucket=args.bucket, prefix=args.prefix)

    # Top level is event types
    event_types = folders

    start_date = datetime.strptime(args.start, "%m/%d/%Y %H")
    end_date = datetime.strptime(args.end, "%m/%d/%Y %H")

    for event_type in event_types:
        logging.info(event_type.get("Prefix"))
        # Within an event type, iterate over date range by hour
        for single_date in hourrange(start_date, end_date):
            daypk = single_date.strftime("%Y%m%d")
            hourpk = single_date.strftime("%H")
            prefix = (
                f"{event_type.get('Prefix')}uploadedDPK={daypk}/uploadedHPK={hourpk}/"
            )

            files, folders = list_files(s3, bucket=args.bucket, prefix=prefix)
            if len(files) > 0 or len(folders) > 0:
                logging.info(f"  {prefix}")
                logging.info(f"    Files: {len(files)}  Folders: {len(folders)}")
                files_md = parse_s3_metadata(
                    files, daypk, hourpk, event_type.get("Prefix").split("/")[2]
                )
                logging.info("     Downloading...")
                download_files(s3, args.bucket, args.localpath, files_md)


if __name__ == "__main__":
    main()
