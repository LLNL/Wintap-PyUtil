## START HERE: process_uber_summary.parquet
 * This table is summarized to PID_HASH for every process instance in the time range.
 * It has summarized information for process, file, network, registry, and DLLs activity.
 * It also has sigma, mitre, lolBAS and manual labels.
 
 Generally, this table is the best place to start as it has so much varied information. Researchers often later dive deeper into the various supporting tables.

## LABELS
Datasets are labeled in 2 ways: manually by humans (labels) and programmatically by scripts (lolbas, mitre, and sigma).

Manual labels:
For manual labels, we work with the red team attackers and develop a graph of activity composed of processes, files, network, etc. These graphs are persisted as NetworkX JSON format and are in the "Sources" directory. From these graphs, we extract the unique identifiers and ultimately create a summarized table that can be joined to PROCESS using the PID_HASH.

With the addition of Caldera attacks, we can now parse reports generated from Caldera that detail the execution activity. That data is converted into a networkx graph and processed along with manual labels described above.

Scripted labels are created several different ways:
* Sigma/Mitre labels are created by running publicly avaliable rules. The hits are quite large for both, producing many false positives.
* LolBAS (Living of the Land) are created by simply joining the LolBAS list of programs, by name, to the PROCESS data. Again, this produces many false positives.
    * (add info about lolc here)

### Labels summarized to PID_HASH can be found in the following files (i.e., one row per PID_HASH):
* labels_graph_process_summary.parquet (manual)
* process_lolbas_summary.parquet (scripted)
* process_mitre_summary.parquet (scripted)
* sigma_labels_summary.parquet (scripted)

### Summaries are derived from more detailed tables (where the same PID_HASH will be found on multiple rows if it got more than one hit):

* labels_graph_process_summary.parquet (manual) <- These are essentially the flattened data extracted from the graphs:
    * labels_graph_net_conn.parquet
    * labels_graph_nodes.parquet
    * labels_networkx.parquet

* process_lolbas_summary.parquet (scripted) <- lolbas.parquet

* process_mitre_summary.parquet (scripted) <- mitre_labels.parquet

* sigma_labels_summary.parquet (scripted) <- sigma_labels.parquet 
