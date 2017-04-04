from pyspark import SparkContext
import sys, os, shutil

sc = SparkContext('local','app')

graph_file = sc.textFile(sys.argv[1])

# function emits list of two tuples representing the actual edge and the edge in reverse with weights +1 and -1 respectively
def getEdgeSpecs(edgeString):
	edge = edgeString.strip().split()
	return [(edge[1], edge[0], -1), (edge[0], edge[1], +1)]



edges = graph_file.flatMap(lambda edge: getEdgeSpecs(edge)) \
			.map(lambda edgeSpec: ((edgeSpec[0],edgeSpec[1]), int(edgeSpec[2]))) \
			.distinct() \
			.reduceByKey(lambda a, b: a+b) \
			.filter(lambda reduceResult: reduceResult[1]==0) \
			.map(lambda filteredResult: (int(filteredResult[0][0]), [int(filteredResult[0][1])])) \
			.reduceByKey(lambda a, b: sorted(a+b)) \
			.sortByKey()

if os.path.exists(sys.argv[2]):
        shutil.rmtree(sys.argv[2])

edges.saveAsTextFile(sys.argv[2])
