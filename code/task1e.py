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
    ridership = sc.textFile(sys.argv[1], 1) #ridership
    ridership = ridership.mapPartitions(lambda x: reader(x))
    ind = 0
    if sys.argv[2] == "task1b_2019.out":
    	ind = 2
    elif sys.argv[2] == "task1b_2020.out":
    	ind = 1
    ridership = ridership.map(lambda x: (getWeekday(x[0]), x[ind]))

    turnstile = sc.textFile(sys.argv[2], 1) #turnstile
    turnstile = turnstile.mapPartitions(lambda x: reader(x))
    turnstile = turnstile.map(lambda x: (getWeekday(x[0]),(x[0], x[1], x[2])))

    result = ridership.join(turnstile).sortByKey()
    #result = result.map(lambda x: x[1][1][0]+","+x[1][1][1]+","+x[1][1][2]+","+x[1][0])
    result.saveAsTextFile(sys.argv[3])
    sc.stop
