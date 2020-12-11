from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    records = records.mapPartitions(lambda x: reader(x))
    records_2019 = records.filter(lambda x: x[2][6:10]=="2019").filter(lambda x: x[1] != '')
    records_2020 = records.filter(lambda x: x[2][6:10]=="2020").filter(lambda x: x[1] != '')
    data = records_2019+records_2020
    data = data.map(lambda x:((x[4],x[2][6:10]+"-"+x[2][:5].replace("/","-")), int(x[1])))
    data=data.reduceByKey(lambda x, y: x + y)
    data = data.map(lambda x:(x[0][0],(x[0][1], x[1])))


    counter = sc.textFile(sys.argv[2], 1)
    header = counter.first().split(",")
    counter = counter.mapPartitions(lambda x: reader(x))
    counter = counter.filter(lambda x: x[0] != header[0])
    site = header.index("site")
    name = header.index("name")
    counter_data = counter.map(lambda x:(x[site], x[name]))

    result = data.join(counter_data)
    result = result.map(lambda x: (x[1][0][0], x[0], x[1][1], str(x[1][0][1]))).sortBy(lambda x: (x[0], x[1]))
    result = result.map(lambda x: ",".join(x))
    result.saveAsTextFile("task2a.out")
    sc.stop
