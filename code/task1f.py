from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    records = records.mapPartitions(lambda x: reader(x))
    total = records.map(lambda x: (x[0], (x[1], x[2])))

    rides = sc.textFile(sys.argv[2], 1)
    rides = rides.mapPartitions(lambda x: reader(x))
    rides_2020 = rides.map(lambda x:(x[0], x[1]))
    rides_2019 = rides.map(lambda x:(x[0], x[2]))
    rides_sub = rides_2020.join(rides_2019).map(lambda x:(x[0], (x[1][0], x[1][1], int(x[1][1])- int(x[1][0]), str(round(int(x[1][0])*1.0/int(x[1][1]), 2)))))

    result = rides_sub.join(total).map(lambda x: (x[0], x[1][0][0], x[1][0][1], str(x[1][0][2]), x[1][0][3], x[1][1][0], x[1][1][1]))
    result = result.sortBy(lambda x: x[0]).map(lambda x: ",".join(x))
    result.saveAsTextFile("task1f.out")
    sc.stop
