TODO:
* Include a simple diagram of the flow
* Include the help docs from the script. They should be complete and standalone anyway.
* Include a description of why upload vs collected ts are used.
* Instructions for setting up an S3 bucket
* Future: setup for using ceph as s3?

S3 bucket structure is partitioned by upload time (UTC):
   {root}/{event type}/uploadDPK=YYYYMMDD/uploadHPK=HH/{parquet files}

Filenames are:
  {hostname}+{event type}-{collected timestamp in UTC epoch seconds}.parquet

