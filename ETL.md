 


| Program | Aggregation Level | Hosts/file | Timespan/file | Timestamp | Type | ETL |
| --- | --- | --- | --- | --- | --- | --- |
|Wintap|(ProgramData)|1|1 minute|win32|none|
|MergeHelper|raw_sensor|1|5 minute|win32|none|
|Python/SQL|raw|N|<=24 HR|unix|none|
|Python/SQL|rolling|N|<=24 HR|unix|Aggregation per event type, Process unique|
|Python/SQL|stdview(-X)|N|any/all|unix|Aggregation per event type|
