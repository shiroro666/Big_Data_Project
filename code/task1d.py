from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    header = records.first().split(",")
    records = records.mapPartitions(lambda x: reader(x))
    records = records.filter(lambda x: x[0] == "New York City MTA Rail")
    date = header.index("Date")
    data_2020 = header.index("Current")
    data_2019 = header.index("Baseline")
    pairs = records.map(lambda x: (x[date][6:]+"-"+x[date][0:5].replace('/','-'), x[data_2020], x[data_2019])).sortBy(lambda x: x[0])
    pairs = pairs.map(lambda x: x[0]+","+x[1]+","+x[2])
    pairs.saveAsTextFile("task1d.out")
    sc.stop