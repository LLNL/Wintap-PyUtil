import argparse
import logging
import os
import shutil
from importlib.resources import files as resource_files
from pathlib import Path
from typing import Dict, List


def parse_filename(filename, hostname="fromonhost"):
    """
    The format of the filename when initially written doesn't include the hostname, so just a default name.
    TODO: Have wintap write in the default naming to include hostname.
    """
    event_type = filename.split("-")[0]
    # Drop the '.parquet' also
    data_capture_epoch = filename.split("-")[1].split(".")[0]
    return hostname, event_type, data_capture_epoch


def write_to_merged(dataset, dst_dataset):
    for exp in os.listdir(f"{dataset}"):
        src_path = f"{dataset}/wintap"
        if os.path.isdir(src_path):
            for src_filename in os.listdir(src_path):
                if src_filename.endswith(".parquet"):
                    # Rename to filename mergehelper would use:
                    #   hostname+event_type+epoch_ts.parquet
                    (hostname, event_type, data_capture_epoch) = parse_filename(
                        src_filename
                    )
                    # Change event_type if needed
                    match event_type:
                        case "tcpconnection_sensor":
                            event_type = "tcp_process_conn_incr"
                        case "udpconnection_sensor":
                            event_type = "udp_process_conn_incr"
                        case "_":
                            event_type.replace("_sensor", "")

                    # Partition data
                    local_file_path = f"{dst_dataset}/merged"
                    dst_filename = (
                        f"{hostname}+raw_{event_type}+{data_capture_epoch}.parquet"
                    )
                    if not os.path.exists(f"{local_file_path}/{dst_filename}"):
                        # print(f"Exists, skipping: {dst_filename}")
                        #                    else:
                        os.makedirs(local_file_path, exist_ok=True)
                        shutil.copy2(
                            f"{src_path}/{src_filename}",
                            f"{local_file_path}/{dst_filename}",
                        )


#                        print(f"{src_filename} -> {dst_filename}")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="onhosttomerged.py",
        description="Rename sensor files into merged format",
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
    write_to_merged(args.source, args.dest)


if __name__ == "__main__":
    main(argv=None)
