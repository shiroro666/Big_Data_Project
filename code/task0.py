from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    cases = sc.textFile(sys.argv[1], 1)
    cases = cases.mapPartitions(lambda x: reader(x))
    cases = cases.map(lambda x: x[0][6:]+"-"+x[0][0:5].replace('/','-')+","+",".join(x[1:]))
    cases.saveAsTextFile("task0.out")
    sc.stop