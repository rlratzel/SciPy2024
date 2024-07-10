# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Enable cudf.pandas:
# python -m cudf.pandas demo5.py
#
# Enable nx-cugraph:
# NETWORKX_BACKEND_PRIORITY="cugraph" python demo5.py
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
edgelist_csv = "full_graph.csv"
nodedata_csv = "full_data.csv"
revisions_csv = "halved_revisions.csv"

with Timer(f"Read the Wikipedia revision history from {revisions_csv}"):
    revisions_df = pd.read_csv(
        revisions_csv,
        sep="\t",
        names=["title", "editor"],
        dtype="str",
    )

with Timer(f"Read the Wikipedia page metadata from {nodedata_csv}"):
    nodedata_df = pd.read_csv(
        nodedata_csv,
        sep="\t",
        names=["nodeid", "title"],
        dtype={"nodeid": "int32", "title": "str"},
    )

with Timer(f"Connect page editors to the page ids"):
    node_revisions_df = nodedata_df.merge(revisions_df, on="title")

with Timer(f"Read the Wikipedia connectivity information from {edgelist_csv}"):
    edgelist_df = pd.read_csv(
        edgelist_csv,
        sep=" ",
        names=["src", "dst"],
        dtype="int32",
    )

with Timer(f"Create a NetworkX graph from the connectivity info"):
    G = nx.from_pandas_edgelist(
        edgelist_df,
        source="src",
        target="dst",
        create_using=nx.DiGraph,
    )

with Timer(f"Run NetworkX pagerank"):
    nx_pr_vals = nx.pagerank(G)

if os.environ.get("NETWORKX_BACKEND_PRIORITY") is not None:
    with Timer(f"Run again using the cached graph conversion"):
        nx.pagerank(G, backend="cugraph")

with Timer(f"Create a DataFrame containing PageRank values"):
    pagerank_df = pd.DataFrame({
        "nodeid": nx_pr_vals.keys(),
        "pagerank": nx_pr_vals.values()
    })

with Timer(f"Merge the PageRank scores onto the per-page information"):
    final_df = node_revisions_df.merge(pagerank_df, on="nodeid").drop("nodeid", axis=1)

with Timer(f"Compute the most influential editors"):
    influence = final_df[['editor', 'pagerank']].groupby("editor").sum().reset_index()

with Timer(f"Show the most influential human editors"):
    most_influential_human = influence[~influence["editor"].str.lower().str.contains("bot")]
    print(most_influential_human.sort_values(by="pagerank").tail(10))


# Six Degrees of SciPy
other_articles = [
    "Orange juice",
    "Lake Leon (Florida)",
    "Kevin Bacon",
]

with Timer(f"Find the nodeids for articles in the nodedata"):
    scipy_nodeid = nodedata_df.loc[nodedata_df["title"] == "SciPy"]["nodeid"].values[0]
    other_nodeids = {t: nodedata_df.loc[nodedata_df["title"] == t]["nodeid"].values[0]
                     for t in other_articles}

with Timer(f"Find the shortest path between the SciPy article and all articles"):
    nx_shortest_paths = nx.shortest_path(G, source=scipy_nodeid)

with Timer("Print the shortest paths"):
    for p in other_nodeids:
        print(f"\nFind the shortest path between SciPy and {p}...")
        for nodeid in nx_shortest_paths[other_nodeids[p]]:
            print(f'{nodedata_df.loc[nodedata_df["nodeid"] == nodeid]["title"].values[0]}')
