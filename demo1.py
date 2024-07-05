# Enable cudf.pandas:
# python -m cudf.pandas demo1.py
#
# Enable nx-cugraph:
# NETWORKX_BACKEND_PRIORITY="cugraph" python demo1.py
#
import os
import time
from datetime import timedelta

import cudf.pandas
cudf.pandas.install()

import pandas as pd
import networkx as nx


class Timer:
    def __init__(self, start_msg=""):
        self.st = 0
        self.start_msg = start_msg
    def __enter__(self):
        if self.start_msg:
            print(f"\n{self.start_msg}...")
        self.st = time.perf_counter()
    def __exit__(self, exc_type, exc_value, traceback):
        runtime = time.perf_counter() - self.st
        print(f"Done in: {timedelta(seconds=runtime)}")

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

with Timer(f"Create a NetworkX graph from the connectivity info"):
    G = nx.from_pandas_edgelist(
        edgelist_df,
        source="src",
        target="dst",
        create_using=nx.DiGraph,
    )

with Timer(f"Run NetworkX pagerank"):
    nx_pr_results = nx.pagerank(G)

if os.environ.get("NETWORKX_BACKEND_PRIORITY") is not None:
    with Timer(f"Run again using the cached graph conversion"):
        nxcg_pr_results = nx.pagerank(G, backend="cugraph")

with Timer(f"Add pagerank results to nodedata as a new column"):
    #nodedata_df["pagerank"] = nodedata_df["nodeid"].map(nx_pr_results)
    nodedata_df.set_index("nodeid", inplace=True)
    pagerank_df = pd.DataFrame(nx_pr_results.items(), columns=["nodeid", "pagerank"])
    pagerank_df.set_index("nodeid", inplace=True)
    nodedata_df = nodedata_df.join(pagerank_df)

with Timer(f"Show the top 25 pages based on pagerank score"):
    print(nodedata_df.sort_values(by="pagerank", ascending=False).head(25))
