# Overview

Current status: these parquet templates are legacy/supporting artifacts. The dbt pipeline increasingly uses typed empty SQL models for missing optional data, especially for Spark/S3 where copying local template files is not appropriate.

This directory contains empty parquet files that match the current schema from Wintap.

The intent is that these can be used as stubs when data is missing from a collect. With
one of these in place, SQL processing that relies on the structure will execute.

# Creating new versions of these
When the Wintap schema is altered, a new template file here may be required. Creating a new
one is a fairly simple process, as follows:

* Using duckdb, read the new file into a table, but with no data:
```
create view template as from 'new.parquet' where false;
```
* Write that table back out as parquet
```
COPY template TO 'new-empty.parquet' (FORMAT 'parquet');
```

That's it! 

