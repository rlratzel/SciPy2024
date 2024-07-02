import time
import xml.etree.ElementTree as ET
from enum import Enum
import mwparserfromhell as mwp

parserStateEnum = Enum("parserStateEnum",
                       "lookingForNextPage foundPage")


def getLinksFromText(title, text):
    parser = mwp.parse(text)
    return [str(l.title) for l in parser.filter_wikilinks()]

def convertSecondsToHumanReadable(s):
    hours = s / 3600
    minutes = (s / 60) % 60
    seconds = s % 60
    return "%02d:%02d:%02d" % (hours, minutes, seconds)

def processXmlFile(xmlFileName):
    """Returns:
    adjacencyListMap - map of vertex id (array index) to list of vertex ids it
    links to
    titleIndexMap - map of vertex id to wikipedia title
    """

    # titleindexmap contains both titles to indexes and indexes to titles.
    # This works because titles are always strings and indexes are always ints.
    titleIndexMap = {}
    adjacencyListMap = {}
    redirectMap = {}
    lastIndex = 0
    parserState = parserStateEnum.lookingForNextPage
    title = None
    processingPage = False

    print("Reading: %s" % xmlFileName)
    st = time.time()
    intermediateTime = st
    numPagesToIssueUpdate = 1000
    i=0

    for (event, elem) in ET.iterparse(xmlFileName, events=("start", "end")):
        # open tag, not all data read in yet
        if (event == "start"):
            # a new page is being processed, so change the parser state so when
            # specific tags are encountered they wont be ignored
            if elem.tag.endswith("page"):
                processingPage = True

        # close tag, all data should be present here
        elif (event == "end"):
            if elem.tag.endswith("page"):
                processingPage = False
                i+=1
                if (i%numPagesToIssueUpdate) == 0:
                    now = time.time()
                    print("processed %d pages in %s seconds (%d total pages in %s)..." % \
                          (numPagesToIssueUpdate, now-intermediateTime, i, convertSecondsToHumanReadable(now-st)))
                    intermediateTime = now

            elif processingPage:
                if elem.tag.endswith("title"):
                    title = str(elem.text)

                elif title:
                    if elem.tag.endswith("redirect"):
                        redirectedTitle = title
                        actualTitle = str(elem.attrib["title"])

                        actualTitleIndex = titleIndexMap.get(actualTitle)
                        # if actualTitle has not been processed yet, assign it a
                        # new ID and add it to the map.  When it gets processed,
                        # this ID will be used instead.
                        if actualTitleIndex is None:
                            actualTitleIndex = lastIndex
                            lastIndex += 1
                            titleIndexMap[actualTitle] = actualTitleIndex
                            titleIndexMap[actualTitleIndex] = actualTitle

                        # check if the redirectedTitle was used before (ie. a
                        # page linked to the redirected title instead of the
                        # actual) and re-use that index if present, otherwise
                        # just use the actualTitleIndex
                        redirectedTitleIndex = titleIndexMap.get(redirectedTitle)
                        if redirectedTitleIndex is None:
                            #redirectedTitleIndex = lastIndex
                            #lastIndex += 1
                            #titleIndexMap[redirectedTitle] = redirectedTitleIndex
                            #titleIndexMap[redirectedTitleIndex] = redirectedTitle
                            titleIndexMap[redirectedTitle] = actualTitleIndex
                        elif redirectedTitleIndex != actualTitleIndex:
                            # save the redirect for post-processing, where all
                            # redirected indexes are replaced with actuals
                            redirectMap[redirectedTitleIndex] = actualTitleIndex

                        processingPage = False
                        title = None

                    elif elem.tag.endswith("text"):
                        #print("processed %s" % title)

                        titleIndex = titleIndexMap.get(title)
                        if titleIndex is None:
                            titleIndex = lastIndex
                            lastIndex += 1
                            titleIndexMap[title] = titleIndex
                            titleIndexMap[titleIndex] = title

                        adjList = []
                        for linkTitle in getLinksFromText(title, elem.text):
                            linkTitleIndex = titleIndexMap.get(linkTitle)
                            # if a link title has already been processed, use
                            # that index, otherwise create a new one for the
                            # link title and add it to the map.
                            if linkTitleIndex is None:
                                linkTitleIndex = lastIndex
                                lastIndex += 1
                                titleIndexMap[linkTitle] = linkTitleIndex
                                titleIndexMap[linkTitleIndex] = linkTitle

                            adjList.append(linkTitleIndex)

                        adjacencyListMap[titleIndex] = adjList
                        processingPage = False
                        title = None
                        #if titleIndex==0:
                        #    print("adj list for 0, title %s, is %s" % (title,adjacencyListMap[0]))
                        #    break

            elem.clear()

    # replace all IDs that were created for redirected pages with the IDs of the
    # actual pages using the redirectMap
    print("post-processing redirects...")
    pt=time.time()
    redirectedIndexes = set(redirectMap.keys())
    # replace all redirected indexes in the adj lists with the actual indexes
    for adjList in adjacencyListMap.values():
        for i in range(len(adjList)):
            if adjList[i] in redirectedIndexes:
                adjList[i] = redirectMap[adjList[i]]
    # remove all redirected indexes from the titleIndexMap
    for i in redirectedIndexes:
        titleIndexMap.pop(i, None)
    # replace all redirected indexes that link titles may be mapped to with the
    # actual indexes
    for (k, v) in titleIndexMap.items():
        titleIndexMap[k] = redirectMap.get(v, v) # if v is a redirect get the actual, otherwise just return v
    print("Done postprocessing in %s seconds." % (time.time()-pt))
    print("Done processing XML in %s seconds." % (time.time()-st))
    return (adjacencyListMap, titleIndexMap)


if __name__ == "__main__":
    import sys
    xmlFileName = sys.argv[1]
    csvOutFileName = sys.argv[2]
    nodeNamesOutFileName = sys.argv[3]

    (adjacencyListMap, titleIndexMap) = processXmlFile(xmlFileName)

    csvOut = open(csvOutFileName, "w")
    namesOut = open(nodeNamesOutFileName, "w")

    # write CSV out: two columns (src, dst)
    for (src, destinations) in adjacencyListMap.items():
        for d in destinations:
            csvOut.write("%d %d\n" % (src, d))
    csvOut.close()

    # write out map of node indexes to wikipedia titles
    for (key, value) in titleIndexMap.items():
        # only print index->title mapping for file size, reverse map can be made
        # at load.
        if isinstance(key, int):
            namesOut.write("%d:::\"\"\"%s\"\"\"\n" % (key, repr(value)))
    namesOut.close()

    #tree = ET.parse(xmlFileName)
    #root = tree.getroot()
    """
        Links:
        "[[<page title>]]"  example: [[Political philosophy]]
        "[[<page title>|<text in page>]]"  example: [[Political philosophy|political]]
        References:
        "{{main|<page title>}}"  example: {{main|Metaphysics (Aristotle)}}
        "{{see|<page title>}}"  example: {{see | Hylomorphism }}
        "{{further|<page title>}}"  example: {{further | History of optics}}
    {{see also | Commentaries on Aristotle | Byzantine Aristotelianism}}
    {{further|Logic in Islamic philosophy|Transmission of the Greek Classics}}
    {{details | Ancient Greek medicine}}
    """
