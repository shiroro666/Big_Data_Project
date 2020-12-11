from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile("task0.out", 1)
    records = records.mapPartitions(lambda x: reader(x)).map(lambda x: (x[0], (x[1], x[2])))

    bikes = sc.textFile("task3b_2020.out", 1)
    bikes = bikes.mapPartitions(lambda x: reader(x)).map(lambda x: (x[0], (x[1], x[2])))

    result=bikes.join(records).map(lambda x:(x[0], x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1])).sortBy(lambda x:x[0])
    result = result.map(lambda x: ','.join(x))
    result.saveAsTextFile("task3d.out")
    sc.stop
