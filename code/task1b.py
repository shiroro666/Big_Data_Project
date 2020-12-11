from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    records = records.mapPartitions(lambda x: reader(x))
    entry_pair = records.map(lambda x: (x[0], int(x[2])))
    entry_pair = entry_pair.reduceByKey(lambda x, y: x + y)

    exit_pair = records.map(lambda x: (x[0], int(x[3])))
    exit_pair = exit_pair.reduceByKey(lambda x, y: x + y)

    result = entry_pair.join(exit_pair).sortByKey()
    result = result.map(lambda x: x[0]+ ","+str(x[1][0])+","+str(x[1][1]))
    result.saveAsTextFile(sys.argv[2])
    sc.stop
