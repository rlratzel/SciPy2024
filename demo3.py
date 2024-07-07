# Enable cudf.pandas:
# python -m cudf.pandas demo1.py
#
# Enable nx-cugraph:
# NETWORKX_BACKEND_PRIORITY="cugraph" python demo1.py
#
import os
import time
from datetime import timedelta

import pandas as pd
import networkx as nx


class Timer:
    session_total = 0

    def __init__(self, start_msg=""):
        self.st = 0
        self.start_msg = start_msg

    def __enter__(self):
        if self.start_msg:
            print(f"\n{self.start_msg}...")
        self.st = time.perf_counter()

    def __exit__(self, exc_type, exc_value, traceback):
        runtime = time.perf_counter() - self.st
        Timer.session_total += runtime
        print(f"Done in: {timedelta(seconds=runtime)}")

    @classmethod
    def print_total(cls):
        print(f"Total time: {timedelta(seconds=cls.session_total)}")


nx.config.cache_converted_graphs = True  # This is the default in NX 3.4

# wget https://dumps.wikimedia.org/enwiki/20240620/enwiki-20240620-pages-articles-multistream.xml.bz2
# run wikipedia2csv_3.py
edgelist_csv = "enwiki-20240620-edges_2.csv"
nodedata_csv = "enwiki-20240620-nodeids_2_2.csv"

with Timer(f"Read the wikipedia connectivity information from {edgelist_csv}"):
    edgelist_df = pd.read_csv(
        edgelist_csv,
        sep=" ",
        names=["src", "dst"],
        dtype="int32",
    )

with Timer(f"Read the wikipedia page metadata from {nodedata_csv}"):
    nodedata_df = pd.read_csv(
        nodedata_csv,
        sep="\t",
        names=["nodeid", "title"],
        dtype={"nodeid": "int32", "title": "str"},
    )

"""
wp_namespace_filter = {
    "User:", "Wikipedia:", "Project:", "File:", "Image:", "MediaWiki:", "Template:",
    "Help:", "Category:", "Portal:", "Draft:", "TimedText:", "Module:",
}
wp_namespace_filter.update({ns[:-1] + " talk:" for ns in wp_namespace_filter})
wp_namespace_filter.update({"WP:", "WT:", "TM:"})
# Keeping {Category:, Portal:}, but not the talk namespaces for each
wp_namespace_filter.difference_update({"Category:", "Portal:"})
# Titles are in quotes, so add leading quote to match using .startswith()
wp_namespace_filter = tuple(f"\"\'{ns}" for ns in wp_namespace_filter) + \
                      tuple(f"\'\"{ns}" for ns in wp_namespace_filter)


print(f"\nNumber of links: {len(edgelist_df)}")
with Timer(f"Remove pages not in the main namespace"):
    nodeids_to_remove = set(nodedata_df[nodedata_df["title"].str.startswith(wp_namespace_filter)]["nodeid"])
    edgelist_df = edgelist_df[~edgelist_df["src"].isin(nodeids_to_remove)]
    edgelist_df = edgelist_df[~edgelist_df["dst"].isin(nodeids_to_remove)]
print(f"\nnumber of nodeids to remove: {len(nodeids_to_remove)}")
print(f"Number of links: {len(edgelist_df)}")
"""

"""
import nx_cugraph as nxcg
with Timer(f"Create a nx-cugraph graph from the connectivity info"):
    Gcg = nxcg.from_pandas_edgelist(
        edgelist_df,
        source="src",
        target="dst",
        create_using=nx.DiGraph,
    )
# Time with cudf.pandas:     0:03:34.370534
# Time without cudf.pandas:  0:03:26.826157
"""

with Timer(f"Create a NetworkX graph from the connectivity info"):
    G = nx.from_pandas_edgelist(
        edgelist_df,
        source="src",
        target="dst",
        create_using=nx.DiGraph,
    )
"""
with Timer(f"Run NetworkX PageRank"):
    nx_pr_vals = nx.pagerank(G)

if os.environ.get("NETWORKX_BACKEND_PRIORITY") is not None:
    with Timer(f"Run again using the cached graph conversion"):
        nxcg_pr_vals = nx.pagerank(G, backend="cugraph")

with Timer(f"Create a DataFrame containing NetworkX results"):
    nx_results = pd.DataFrame([items for iitems in nx_pr_vals.items()],
                              columns=["nodeid", "pagerank"])

with Timer(f"Add NetworkX results to nodedata as new columns"):
    nodedata_df = nodedata_df.merge(nx_results, how="left", on="nodeid")

with Timer(f"Show the top 25 pages based on PageRank value"):
    print(nodedata_df.sort_values(by="pagerank", ascending=False).head(25))
"""
with Timer(f"Find the nodeids for two articles in the nodedata"):
    scipy_nodeid = nodedata_df.loc[nodedata_df["title"] == "\"\'SciPy\'\""]["nodeid"].values[0]
    orange_juice_nodeid = nodedata_df.loc[nodedata_df["title"] == "\"\'Orange juice\'\""]["nodeid"].values[0]

with Timer(f"Find the shortest path between the two articles"):
    shortest_path = nx.shortest_path(G, source=scipy_nodeid, target=orange_juice_nodeid)

with Timer(f"convert nodeids in the path to page titles and print the path"):
    for nodeid in shortest_path:
        print(f'{nodedata_df.loc[nodedata_df["nodeid"] == nodeid]["title"].values[0]}')

with Timer(f"Find the shortest path between the SciPy article and all articles"):
    nx_shortest_paths = nx.shortest_path(G, source=scipy_nodeid)

with Timer(f"Create a DataFrame containing nodeids and hops from the SciPy article"):
    hops_df = pd.DataFrame([(nodeid, len(nx_shortest_paths[nodeid]) - 1)
                            for nodeid in nx_shortest_paths], columns=["nodeid", "hops_from_scipy"])

with Timer(f"Add hops to nodedata as new columns"):
    nodedata_df = nodedata_df.merge(hops_df, how="left", on="nodeid")

# groupby hops


Timer.print_total()
