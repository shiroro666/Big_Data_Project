from csv import reader
import sys
from pyspark import SparkContext
from datetime import date

def getWeekday(s):
	info=s.split("-")
	weekday = date(int(info[0]), int(info[1]), int(info[2])).isocalendar()
	result = (weekday[1], weekday[2])
	return result

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    records = records.mapPartitions(lambda x: reader(x))
    records = records.map(lambda x:(x[0], int(x[3]))).reduceByKey(lambda x, y: x + y)
    records_2019 = records.filter(lambda x: x[0][0:4] == "2019")
    records_2020 = records.filter(lambda x: x[0][0:4] == "2020")
    records_2019 = records_2019.map(lambda x:(getWeekday(x[0]), (x[0], x[1])))
    records_2020 = records_2020.map(lambda x:(getWeekday(x[0]), (x[0], x[1])))
    result = records_2020.join(records_2019)
    result = result.map(lambda x:(x[1][0][0], x[1][0][1], x[1][1][1])).sortBy(lambda x: x[0])
    result = result.map(lambda x:x[0] +","+str(x[1])+","+str(x[2]))
    result.saveAsTextFile("task2b.out")
    sc.stop
