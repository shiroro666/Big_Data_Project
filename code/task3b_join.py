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
    pre = sc.textFile("task3b_2019.out", 1) #ridership
    pre = pre.mapPartitions(lambda x: reader(x))
    pre = pre.map(lambda x: (getWeekday(x[0]), (x[0], x[1], x[2])))

    curr = sc.textFile("task3b_2020.out", 1) #ridership
    curr = curr.mapPartitions(lambda x: reader(x))
    curr = curr.map(lambda x: (getWeekday(x[0]), (x[0], x[1], x[2])))

    result = curr.join(pre).map(lambda x:(x[1][0][0], x[1][0][1], x[1][0][2], x[1][1][1], x[1][1][2]))
    #result = result.map(lambda x: x[1][1][0]+","+x[1][1][1]+","+x[1][1][2]+","+x[1][0])
    result=result.map(lambda x: ",".join(x)).sortBy(lambda x: x[0])
    result.saveAsTextFile("task3b.out")
    sc.stop
