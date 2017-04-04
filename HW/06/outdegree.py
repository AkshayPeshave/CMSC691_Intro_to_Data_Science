from pyspark import SparkContext
import sys, os, shutil

sc = SparkContext('local','app')

graph_file = sc.textFile(sys.argv[1])

outdegrees = graph_file.flatMap(lambda edge: [edge.strip().split('\t')[0]]) \
			.map(lambda node: (int(node), 1)) \
			.reduceByKey(lambda a, b: a+b) \
			.sortByKey()

if os.path.exists(sys.argv[2]):
	shutil.rmtree(sys.argv[2])
outdegrees.saveAsTextFile(sys.argv[2])
