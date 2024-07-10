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

with Timer(f"Run NetworkX pagerank"):
    nx_pr_vals = nx.pagerank(G)

if os.environ.get("NETWORKX_BACKEND_PRIORITY") is not None:
    with Timer(f"Run again using the cached graph conversion"):
        nxcg_pr_vals = nx.pagerank(G, backend="cugraph")

with Timer(f"Run NetworkX HITS"):
    (nx_hits_hubs, nx_hits_authorities) = nx.hits(G)

with Timer(f"Create a DataFrame containing NetworkX results"):
    nx_results = pd.DataFrame([(nodeid, pagerank, nx_hits_hubs[nodeid], nx_hits_authorities[nodeid])
                               for (nodeid, pagerank) in nx_pr_vals.items()],
                              columns=["nodeid", "pagerank", "hub_val", "auth_val"])

with Timer(f"Add NetworkX results to nodedata as new columns"):
    nodedata_df = nodedata_df.merge(nx_results, how="left", on="nodeid")

with Timer(f"Show the top 25 pages based on pagerank value"):
    print(nodedata_df.sort_values(by="pagerank", ascending=False).head(25))

with Timer(f"Show the top 25 pages based on HITS hub value"):
    print(nodedata_df.sort_values(by="hub_val", ascending=False).head(25))

with Timer(f"Show the top 25 pages based on HITS authority value"):
    print(nodedata_df.sort_values(by="auth_val", ascending=False).head(25))

Timer.print_total()
