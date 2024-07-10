import cudf
import cugraph

def read_csv_file(csv_file):
    return cudf.read_csv(csv_file, delimiter=' ',
                         dtype=['int32', 'int32'], header=None)


def read_title_file(title_file):
    retDict = {}
    i = 0
    with open(title_file) as f:
        for line in f:
            i += 1
            try:
                lineParts = line.split("\t")
                vertexNum = lineParts[0]
                # title could have the delim ("\t") in it, so concat all
                # remaining parts that were split on delim back together using
                # the delim
                title = "\t".join(lineParts[1:]).strip()
            except:
                print("Error reading line %d: %s" % (i, line))
                raise
            # remove triple quotes around title
            title = title[3:-3]
            retDict[vertexNum] = title
            retDict[title] = vertexNum
    return retDict


if __name__ == "__main__":
    import sys
    import time

    st = time.time()
    gdf = read_csv_file(sys.argv[1])
    print("read csv in %s seconds" % (time.time() - st))
    st = time.time()
    pageTitleMap = read_title_file(sys.argv[2])
    print("read page titles in %s seconds" % (time.time() - st))

    G = cugraph.Graph(directed=True)
    G.from_cudf_edgelist(gdf, source='0', destination='1')

    def sssp(pageTitle):
        return cugraph.sssp(G, int(pageTitleMap[pageTitle]))

    def printPath(distances, pageTitle):
        path = [pageTitle]
        vertex=pageTitleMap[pageTitle]
        r = distances.query('vertex==%d' % int(vertex))
        p = r['predecessor'][0]
        while(p != -1):
            path.insert(0,pageTitleMap['%d' % p])
            r = distances.query('vertex==%d' % int(p))
            p = r['predecessor'][0]
        for t in path:
            print("\t%s" % t)

    print("Using a directed Graph of Wikipedia data from Feb. 2020 to trace the relationships between things:")
    print("-" * 10)
    print("How the Chevrolet Corvette is related to...")
    corvetteDistances = sssp("'Chevrolet Corvette'")

    print("   Ansel Adams:")
    printPath(corvetteDistances, "'Ansel Adams'")
    print()
    print("   Spatulas:")
    printPath(corvetteDistances, "'Spatula'")
    print()
    print("   Coronavirus:")
    printPath(corvetteDistances, "'Coronavirus'")

    print()

    print("-" * 10)
    print("How the Coronavirus is related to...")
    covidDistances = sssp("'Coronavirus'")

    print("   Ansel Adams:")
    printPath(covidDistances, "'Ansel Adams'")
    print()
    print("   Spatulas:")
    printPath(covidDistances, "'Spatula'")
    print()
    print("   Chevrolet Corvette:")
    printPath(covidDistances, "'Chevrolet Corvette'")
