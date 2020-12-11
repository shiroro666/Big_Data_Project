from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    if sys.argv[1] == "2019":
        files = ["JC-201903-citibike-tripdata.csv", "JC-201904-citibike-tripdata.csv", "JC-201905-citibike-tripdata.csv", "JC-201906-citibike-tripdata.csv", "JC-201907-citibike-tripdata.csv", "JC-201908-citibike-tripdata.csv", "JC-201909-citibike-tripdata.csv", "JC-201910-citibike-tripdata.csv", "JC-201911-citibike-tripdata.csv"]
        output = "task3a_2019.out"
    elif sys.argv[1] == "2020":
        files = ["JC-202003-citibike-tripdata.csv", "JC-202004-citibike-tripdata.csv", "JC-202005-citibike-tripdata.csv", "JC-202006-citibike-tripdata.csv", "JC-202007-citibike-tripdata.csv", "JC-202008-citibike-tripdata.csv", "JC-202009-citibike-tripdata.csv", "JC-202010-citibike-tripdata.csv", "JC-202011-citibike-tripdata.csv"]
        output = "task3a_2020.out"
    sc = SparkContext()
    records = sc.textFile(files[0], 1)
    header = records.first().split(",")
    records = records.mapPartitions(lambda x: reader(x))
    records = records.filter(lambda x: x[0] != "tripduration").filter(lambda x: x[0] != "\"tripduration\"")
    records = records.map(lambda x:(x[1][:10], x[0]))
    for i in range(1, 9):
        temp = sc.textFile(files[i], 1)
        header = temp.first().split(",")
        temp = temp.mapPartitions(lambda x: reader(x))
        temp = temp.filter(lambda x: x[0] != "tripduration").filter(lambda x: x[0] != "\"tripduration\"")
        temp = temp.map(lambda x:(x[1][:10], x[0]))
        records += temp

    result = records.map(lambda x: ",".join(x))
    result.saveAsTextFile(output)
    sc.stop