"""
Functions for creating graph data structures from wintap dataframes
"""
import networkx as nx
import pandas as pd


def add_proc_node_for(g, pid_hashes, procdf):
    proc = procdf.loc[procdf["pid_hash"].isin(pid_hashes)]
    print(f"Adding {proc.shape[0]} process nodes")
    for _, row in proc.iterrows():
        add_node(
            g, row["pid_hash"], "process", f"{row['process_name']}\n ({row['os_pid']})"
        )


def get_parent_pid_hashes(processdf):
    """
    Get a list of pid_hashes for parents.
    Note: skip any where parent_pid_hash=pid_hash as that indicates the root
    """
    parent_pid_hashes = processdf["parent_pid_hash"].unique()
    # display(parent_pid_hashes)
    return parent_pid_hashes


def get_children(parent_hashes, process_tree_edges, all_processes):
    """
    Recursively get children, starting with passed pid_hashes.
    TODO: Refactor get_children/get_parents into a single function?
    """
    # print(f'PTE {processTreeEdges.shape[0]}')
    children = all_processes.loc[all_processes["parent_pid_hash"].isin(parent_hashes)]
    # print(f'Parents found: {parents.shape[0]}')
    # display(parents[['pid_hash','parent_pid_hash','process_name','activity_type']])
    if children.shape[0] > 0:
        # Add the new ones to the existing set
        process_tree_edges = pd.concat([process_tree_edges, children])
        # processTreeEdges=processTreeEdges.append(parents)
        # Call for another round of parents, removing any roots
        process_tree_edges = get_children(
            children["pid_hash"].unique(),
            process_tree_edges,
            all_processes,
        )
    else:
        if len(parent_hashes) > 0:
            # Parent is missing
            print(f"Missing parent for: {parent_hashes}")

    process_tree_edges.set_index("pid_hash", drop=False, inplace=True)
    return process_tree_edges


def get_parents(parent_hashes, process_tree_edges, all_processes):
    """
    Recursively get parents, starting with seed processes.
    This function uses pandas and returns a panda DF with all processes. Processes may be duplicated in the result.
    De-duping is expected to happen when the panda is loaded into a networkx graph using "from_pandas_edgelist" or
    when adding to a graph as networkx implicitly de-dupes based on the node/edge ID.
    """
    # print(f'PTE {processTreeEdges.shape[0]}')
    parents = all_processes.loc[all_processes["pid_hash"].isin(parent_hashes)]
    # print(f'Parents found: {parents.shape[0]}')
    # display(parents[['pid_hash','parent_pid_hash','process_name','activity_type']])
    if parents.shape[0] > 0:
        # Add the new ones to the existing set
        process_tree_edges = pd.concat([process_tree_edges, parents])
        # processTreeEdges=processTreeEdges.append(parents)
        # Call for another round of parents, removing any roots
        process_tree_edges = get_parents(
            get_parent_pid_hashes(
                parents[parents["parent_pid_hash"] != parents["pid_hash"]]
            ),
            process_tree_edges,
            all_processes,
        )
    else:
        if len(parent_hashes) > 0:
            # Parent is missing
            print(f"Missing parent for: {parent_hashes}")

    process_tree_edges.set_index("pid_hash", drop=False, inplace=True)
    return process_tree_edges


def add_parent_child(g, procdf):
    """
    Add parent-child edges and nodes for each process passed to the graph.
    """
    for _, row in procdf.iterrows():
        add_proc_node_for(g, [row["pid_hash"], row["parent_pid_hash"]], procdf)
        g.add_edge(row["pid_hash"], row["parent_pid_hash"])


def get_node_type(row):
    # print(row)
    row_type = "Parent"
    if row["process_name"] == "mattermost.exe":
        row_type = "Seed"
    if row["pid_hash"] == row["parent_pid_hash"]:
        row_type = "Root"

    return row_type


def add_all_file_activity(graph, process_file_df):
    for _, row in process_file_df.iterrows():
        # Only display the last 15 chars of filename
        add_node(
            graph, idx=row["file_hash"], label=row["filename"][:-15], nodetype="file"
        )
        # TODO: Figure out how to have a "global" reference for procDF so it doesn't have to be passed here.
        # addProcNodeFor(graph, [row['pid_hash']] )
        # For now, add a proxy node
        add_node(graph, idx=row["pid_hash"], label="Proxy", nodetype="proxyProcess")
        graph.add_edge(
            row["pid_hash"], row["file_hash"], activity_type=row["activity_type"]
        )


def add_file_activity(pid_hashes, graph, process_file_df):
    """
    Add all file activity from processFileDF to the graph for the given pid_hashes
    """
    pfdf = process_file_df.loc[process_file_df["pid_hash"].isin(pid_hashes)]
    # Are there missing hostnames? Fill them in with...
    pfdf[["hostname"]] = pfdf[["hostname"]].fillna("NO_HN")  # Specific columns
    add_all_file_activity(graph, pfdf)


def add_node(graph, idx, nodetype, label, attributes={}):
    """
    Add a node with the given properties.
    This is to ensure all nodes have minimal common attributes
    """
    graph.add_node(idx, label=label, type=nodetype)
    # TODO: Figure out the pythonic way to do this.
    # Now set the other attributes.
    nx.set_node_attributes(graph, {idx: attributes})
    return graph


def add_all_network_activity(graph, pncdf, procdf):
    """
    Add all network activity in the given processNetConnDF to the graph.
    Add nodes for any new Processes.
    Returns the list of new pid_hashes.
    """
    new_pid_hashes = []
    print(f"AddAll: PNC {pncdf.shape[0]} Proc {procdf.shape[0]}")
    for _, row in pncdf.iterrows():
        add_node(
            graph,
            idx=row["conn_id"],
            nodetype="pnc",
            label=f"{row['local_ip_addr']}->{row['remote_ip_addr']}",
        )
        if not row["pid_hash"] in graph:
            add_proc_node_for(graph, [row["pid_hash"]], procdf)
            new_pid_hashes.append(row["pid_hash"])
            # Nope, this doesn't work... We'd need the full set of pncDF to pass...
            # addNetworkActivity(graph, [row['pid_hash']], pncDF, procDF)
        graph.add_edge(row["pid_hash"], row["conn_id"], protocol=row["protocol"])
        # Hack to handle localhost
        remote_ip = row["remote_ip_addr"]
        if remote_ip == "127.0.0.1":
            remote_ip = f"{row['hostname']}:{remote_ip}"
        add_node(graph, idx=remote_ip, nodetype="ip", label=remote_ip)
        graph.add_edge(row["conn_id"], remote_ip)

    return new_pid_hashes


def add_network_activity(graph, pid_hashes, process_net_conn_df, proc_df, max_pnc=100):
    """
    Add all network activity from processNetConnDF to the graph for the given set of pid_hashes
    Skip all if more than maxPNC passed
    """
    ignore_list = ["svchost.exe", "ntoskrnl.exe"]
    new_pid_hashes = []
    for pid_hash in pid_hashes:
        pncdf = process_net_conn_df.loc[process_net_conn_df["pid_hash"] == pid_hash]
        conn_ids = pncdf["conn_id"].unique()
        other_end = process_net_conn_df.loc[
            process_net_conn_df["conn_id"].isin(conn_ids)
        ]
        if pncdf.shape[0] < max_pnc:
            new_pid_hashes = add_all_network_activity(
                graph, pd.concat([pncdf, other_end]), proc_df
            )
        else:
            print(
                f"Skipping {pncdf.shape[0]} PNC edges from pid_hash {pid_hash} with {len(conn_ids)} conn_ids with {other_end.shape[0]} other ends"
            )
            add_node(
                graph,
                idx="NetBlob-" + pid_hash,
                label=f"NetBlob ({pncdf.shape[0]}",
                nodetype="NetBlob",
            )
            graph.add_edge(pid_hash, "NetBlob-" + pid_hash)

    # Add network activity for the new Processes, but filter out common, high degree nodes that just add noise.
    proc_df = proc_df.loc[
        (proc_df["pid_hash"].isin(pid_hashes))
        & (~proc_df["process_name"].isin(ignore_list))
    ]
    if len(new_pid_hashes) > 0:
        print(f"Adding {len(new_pid_hashes)} nodes")
        add_network_activity(graph, new_pid_hashes, process_net_conn_df, proc_df)
