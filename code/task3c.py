from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    trips = sc.textFile(sys.argv[1], 1)
    if sys.argv[1] == "task3a_2019.out":
    	output = "task3c_2019.out"
    elif sys.argv[1] == "task3a_2020.out":
    	output = "task3c_2020.out"
    trips = trips.mapPartitions(lambda x: reader(x)).map(lambda x:(x[0], int(x[1])))
    trips_total_days=trips.map(lambda x:(x[0], 1)).reduceByKey(lambda x, y: x+y)

    t1 = trips.filter(lambda x: x[1] >= 0 and x[1] <= 300).map(lambda x: (x[0], 1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0], ("range_1", x[1])))
    t2 = trips.filter(lambda x: x[1] > 300 and x[1] <= 600).map(lambda x: (x[0], 1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0], ("range_2", x[1])))
    t3 = trips.filter(lambda x: x[1] > 600 and x[1] <= 900).map(lambda x: (x[0], 1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0], ("range_3", x[1])))
    t4 = trips.filter(lambda x: x[1] > 600 and x[1] <= 1200).map(lambda x: (x[0], 1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0], ("range_4", x[1])))
    t5 = trips.filter(lambda x: x[1] > 1200).map(lambda x: (x[0], 1)).reduceByKey(lambda x,y:x+y).map(lambda x: (x[0], ("range_5", x[1])))

    result = t1+t2+t3+t4+t5
    result = result.join(trips_total_days).map(lambda x: (x[0], x[1][0][0], str(x[1][0][1]), str(x[1][1]), str(round(x[1][0][1]*1.0/x[1][1], 2))))
    result = result.sortBy(lambda x:(x[0], x[1])).map(lambda x: ",".join(x))
    result.saveAsTextFile(output)
    sc.stop


