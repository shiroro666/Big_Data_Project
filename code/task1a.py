from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    header = records.first().split(",")
    records = records.mapPartitions(lambda x: reader(x))
    records = records.filter(lambda x: x[0] != header[0])
    date = header.index("date")
    borough = header.index("borough")
    entries = header.index("entries")
    exits = header.index("exits")
    entry_pair = records.map(lambda x: ((x[date], x[borough]), x[entries]))
    entry_pair = entry_pair.filter(lambda x: x[1] != "").filter(lambda x: x[0][1] != "")
    entry_pair = entry_pair.filter(lambda x: x[1] != "NULL").filter(lambda x: x[0][1] != "NULL")
    entry_pair = entry_pair.map(lambda x: ((x[0][0], x[0][1]), int(x[1])))
    entry_pair = entry_pair.reduceByKey(lambda x, y: x + y)

    exit_pair = records.map(lambda x: ((x[date], x[borough]), x[exits]))
    exit_pair = exit_pair.filter(lambda x: x[1] != "").filter(lambda x: x[0][1] != "")
    exit_pair = exit_pair.filter(lambda x: x[1] != "NULL").filter(lambda x: x[0][1] != "NULL")
    exit_pair = exit_pair.map(lambda x: ((x[0][0], x[0][1]), int(x[1])))
    exit_pair = exit_pair.reduceByKey(lambda x, y: x + y)

    result = entry_pair.join(exit_pair).sortByKey()
    result = result.map(lambda x: x[0][0]+ "," + x[0][1]+ "," + str(x[1][0])+","+str(x[1][1]))
    result.saveAsTextFile(sys.argv[2])
    sc.stop
