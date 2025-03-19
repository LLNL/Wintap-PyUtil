import argparse
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone


def parse_filename(filename):
    """
    Legacy format: hostname=event_type+epoch_ts.parquet
    New format:    hostname+event_type+epoch_ts.parquet
    """
    if "=" in filename:
        # Legacy
        hostname = filename.split("=")[0]
        event_type = filename.split("=")[1].rsplit("+")[0]
        data_capture_epoch = filename.split("=")[1].rsplit("+")[1].split(".")[0]
    else:
        hostname = filename.split("+")[0]
        event_type = filename.split("+")[1]
        # Drop the '.parquet' also
        data_capture_epoch = filename.split("+")[2].split(".")[0]
    return hostname, event_type, int(data_capture_epoch)


def win32_to_epoch(wts):
    return wts / 1e7 - 11644473600


def process_files(dataset):
    for file in os.listdir(f"{dataset}/merged"):
        # Merged directory is flat, so no need for recursive listing.
        if file.endswith(".parquet"):
            (hostname, event_type, data_capture_epoch) = parse_filename(file)
            # Copy to standard structure
            data_capture_ts = datetime.fromtimestamp(
                int(win32_to_epoch(data_capture_epoch)), timezone.utc
            )
            datadpk = data_capture_ts.strftime("%Y%m%d")
            datahpk = data_capture_ts.strftime("%H")
            # Define fully-qualified local name
            if any([x in event_type for x in ["tcp", "udp"]]):
                # Special handling of TCP/UDP
                proto = event_type[4:7]
                # Force the correct supertype
                event_type = "raw_process_conn_incr"
                local_file_path = f"{dataset}/raw_sensor/{event_type}/dayPK={datadpk}/hourPK={datahpk}/proto={proto}"
            else:
                match event_type:
                    case "raw_file":
                        event_type = "raw_process_file"
                    case "raw_processstop":
                        event_type = "raw_process"
                    case "raw_registry":
                        event_type = "raw_process_registry"
                local_file_path = f"{dataset}/raw_sensor/{event_type}/dayPK={datadpk}/hourPK={datahpk}"
            os.makedirs(local_file_path, exist_ok=True)
            shutil.copy2(f"{dataset}/merged/{file}", f"{local_file_path}/{file}")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="mergedtoraw.py",
        description="Implements behavior typically done by wintap upload to S3 and python download from S3",
    )

    parser.add_argument(
        "-s",
        "--source",
        default="/Windows/blah/blah",
    )
    parser.add_argument(
        "-d",
        "--dest",
        default="data/merged",
    )
    args = parser.parse_args()

    logging.info(f"Processing {args.source} to {args.dest}")
    process_files(args.source)


if __name__ == "__main__":
    main(argv=None)
