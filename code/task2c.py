from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    records = records.mapPartitions(lambda x: reader(x))
    counts_2020 = records.map(lambda x:(x[0][:7], int(x[1]))).reduceByKey(lambda x,y:x+y)
    counts_2019 = records.map(lambda x:(x[0][:7], int(x[2]))).reduceByKey(lambda x,y:x+y)
    days = records.map(lambda x:(x[0][:7], 1)).reduceByKey(lambda x,y:x+y)

    avg_2020 = counts_2020.join(days).map(lambda x: (x[0], round(x[1][0]*1.0/x[1][1], 2)))
    avg_2019 = counts_2019.join(days).map(lambda x: (x[0], round(x[1][0]*1.0/x[1][1], 2)))
    result = avg_2020.join(avg_2019).map(lambda x:(x[0], (str(x[1][0]), str(x[1][1]))))
    #result=result.map(lambda x: ",".join(x))

    cases = sc.textFile(sys.argv[2], 1)
    cases = cases.mapPartitions(lambda x: reader(x)).map(lambda x:(x[0][:7], int(x[1])))
    days = cases.map(lambda x:(x[0], 1)).reduceByKey(lambda x, y: x+y)
    cases_total = cases.reduceByKey(lambda x, y: x+y)
    cases_avg = cases_total.join(days).map(lambda x: (x[0], str(round(x[1][0]*1.0/x[1][1], 2))))

    final = result.join(cases_avg).map(lambda x:(x[0], x[1][0][0], x[1][0][1], x[1][1])).sortBy(lambda x: x[0])
    final=final.map(lambda x: ",".join(x))

    final.saveAsTextFile("task2c.out")
    sc.stop



