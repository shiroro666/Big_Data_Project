from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    trips = sc.textFile(sys.argv[1], 1)
    if sys.argv[1] == "task3a_2019.out":
    	output = "task3b_2019.out"
    elif sys.argv[1] == "task3a_2020.out":
    	output = "task3b_2020.out"
    trips = trips.mapPartitions(lambda x: reader(x)).map(lambda x:(x[0], int(x[1])))

    trips_total= trips.reduceByKey(lambda x, y: x+y)
    trips_total_days=trips.map(lambda x:(x[0], 1)).reduceByKey(lambda x, y: x+y)
    trips_avg = trips_total.join(trips_total_days).map(lambda x:(x[0], str(x[1][1]), str(round(x[1][0]*1.0/x[1][1], 2)))).sortBy(lambda x:x[0])

    result = trips_avg.map(lambda x: ",".join(x))
    result.saveAsTextFile(output)
    sc.stop


    