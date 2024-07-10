import cudf
import networkx as nx

revisions_df = cudf.read_csv("halved_revisions.csv", sep="\t", names=["title", "editor"], dtype="str")
nodedata_df = cudf.read_csv("full_data.csv", sep="\t", names=["nodeid", "title"], dtype={"nodeid": "int32", "title": "str"})
node_revisions_df = nodedata_df.merge(revisions_df, on="title")

edgelist_df = cudf.read_csv("full_graph.csv", sep=" ", names=["src", "dst"], dtype="int32")

# G is now an nx_cugraph Graph, not a NetworkX Graph, compatible only with algorithms
# that nx_cugraph supports.
G = nx.from_pandas_edgelist(edgelist_df, source="src", target="dst", create_using=nx.DiGraph, backend="cugraph")
nxcg_pr_vals = nx.pagerank(G)

pagerank_df = cudf.DataFrame({"nodeid": nxcg_pr_vals.keys(), "pagerank": nxcg_pr_vals.values()})
final_df = node_revisions_df.merge(pagerank_df, on="nodeid").drop("nodeid", axis=1)
influence = final_df[['editor', 'pagerank']].groupby("editor").sum().reset_index()
most_influential_human = influence[~influence["editor"].str.lower().str.contains("bot")]
print(most_influential_human.sort_values(by="pagerank").tail(10))
