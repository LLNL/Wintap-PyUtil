# Labels
Datasets are labeled in 2 ways: manually by humans and programmatically.

For manual labels, we work with the red team attackers and develop a graph of activity composed of processes, files, network, etc. These graphs are persisted as NetworkX JSON format and are in the "Sources" directory. From these graphs, we extract the unqique identifiers and ultimately create a summarized table that can be joined to PROCESS using the PID_HASH.

Programmatic labels are created several different ways:

* Sigma/Mitre labels are created by running publicly avaliable rules. The hits are quite large for both, producing many false positives.
* LolBAS (Living of the Land) are created by simply joining the LolBAS list of programs, by name, to the PROCESS data. Again, this produces many false positives.
* Caldera labels are created by extracting unique IDs from the Caldera report 

## PROCESS_UBER_SUMMARY: Global process summary with labels
This table is summarized to PID_HASH for every process instance in the time range. It has summarized information for process, file, network, registry, DLLs, sigma, mitre, lolBAS and manual labels. Generally, this table is the best place to start as it has so much varied information. Inevitably, researchers often later dive deeper into the various supporting tables.

## Labels summarized to PID_HASH
* labels_graph_process_summary.parquet
* process_lolbas_summary.parquet
* process_mitre_summary.parquet
* sigma_labels_summary.parquet

## The summaries are derived from these more detailed tables

### Networkx Graphs
These are essentially the flattened data extracted from the graphs.

labels_graph_net_conn.parquet
labels_graph_nodes.parquet
labels_networkx.parquet

### LolBAS
lolbas.parquet

### Mitre Detail
mitre_labels.parquet

### Sigma Detail
sigma_labels.parquet