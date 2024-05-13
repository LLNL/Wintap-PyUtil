# Dataset Overview
Sensor data is streamed from hosts to some collection point and then periodically, post-processed for more efficient storage and data type conversions using command line tools. Additionally, enrichment processing may create new features such as geolocation, Mitre Attck labels and more.

Analysts have access to the various forms data thru convenience utilities included in the Wintappy package. 

These tools expect a standardized directory structure for the data that is outlined below.

## Paths
The following conventions are expected for data set file organization:
 
```
{​​​​Dataset}​/{​Aggregation Level}​/{Event Type}/{hive partitions}/*.parquet
```

## Dataset
A collection of data from a set of hosts. This could be from a single host for 5 minutes, to thousands of hosts for months. 
This should be a short, descriptive term that is easily translatable to a directory path.

At the top level of a dataset are sibling directories that each contain a copy of the data, in different forms. These are referred to as aggregation levels.
 
## Aggregation Level
Standardized levels of aggregation/post processing of sensor data. (See Confluence overview of post processing). Here they are in order starting from the sensor itself 
(ProgramData\Wintap\parquet) - directly written from the sensor ~1/minute
Maybe change parquet to streaming?
* **raw_sensor** - These are the files as streamed from the sensors to the collection point, no changes at all. At this level, the quantity of tiny volumes quickly becomes inefficient to use directly for prolonged analysis. 
* **rolling** - Batches of **raw_sensor** data are merged by day (dayPK) into larger files. There are now 2 forms of the data with the day partitions:
    * **raw_{event type}** are the **raw_sensor** files simply appended together, no changes.
    * **{event type}** have been aggregrated, de-duped and had data type conversions applied as appropriate by event type.
* **stdview-X** - Rolling partitions merged into a single data set. This further de-duplicates information that may span days, such as processes, host information, and long running network connections. This may be a subset of rolling partitions by time/filtered/etc into a single dataset. There can be multiples of these to support various analytic needs.
* **export** - Exports of a slice of data, often denormalized into a flattened structure for a specific analytic/purpose
* **{other}** - Task or tool specific derivatives. Example: a [DOT](https://en.wikipedia.org/wiki/DOT_(graph_description_language)) file representing a graph view of a subset of data.
 
## Event Type

At this level, the path defines the top level of files that can be opened as a table/dataframe. Depending on the aggregation level, there may be required directory structure levels to support partitioning by time. This is known as [hive partitioning](https://duckdb.org/docs/data/partitioning/hive_partitioning.html).
