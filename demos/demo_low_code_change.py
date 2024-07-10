# Copyright (c) 2024, NVIDIA CORPORATION.

import time
from datetime import timedelta

import cudf
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


edgelist_csv = "full_graph.csv"
nodedata_csv = "full_data.csv"
revisions_csv = "halved_revisions.csv"


with Timer(f"Read the Wikipedia revision history from {revisions_csv}"):
    revisions_df = cudf.read_csv(revisions_csv, sep="\t", names=["title", "editor"], dtype="str")

with Timer(f"Read the Wikipedia page metadata from {nodedata_csv}"):
    nodedata_df = cudf.read_csv(nodedata_csv, sep="\t", names=["nodeid", "title"], dtype={"nodeid": "int32", "title": "str"})

with Timer(f"Connect page editors to the page ids"):
    node_revisions_df = nodedata_df.merge(revisions_df, on="title")

with Timer(f"Read the Wikipedia connectivity information from {edgelist_csv}"):
    edgelist_df = cudf.read_csv(edgelist_csv, sep=" ", names=["src", "dst"], dtype="int32")

# G is now an nx_cugraph Graph, not a NetworkX Graph, compatible only with algorithms
# that nx_cugraph supports.
with Timer(f"Create a NetworkX graph from the connectivity info"):
    G = nx.from_pandas_edgelist(edgelist_df, source="src", target="dst", create_using=nx.DiGraph, backend="cugraph")

with Timer(f"Run NetworkX pagerank"):
    nxcg_pr_vals = nx.pagerank(G)

with Timer(f"Create a DataFrame containing PageRank values"):
    pagerank_df = cudf.DataFrame({"nodeid": nxcg_pr_vals.keys(), "pagerank": nxcg_pr_vals.values()})

with Timer(f"Merge the PageRank scores onto the per-page information"):
    final_df = node_revisions_df.merge(pagerank_df, on="nodeid").drop("nodeid", axis=1)

with Timer(f"Compute the most influential editors"):
    influence = final_df[['editor', 'pagerank']].groupby("editor").sum().reset_index()

with Timer(f"Show the most influential human editors"):
    most_influential_human = influence[~influence["editor"].str.lower().str.contains("bot")]
    print(most_influential_human.sort_values(by="pagerank").tail(10))

Timer.print_total()
