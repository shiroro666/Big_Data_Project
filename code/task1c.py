from csv import reader
import sys
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext()
    records = sc.textFile(sys.argv[1], 1)
    records = records.mapPartitions(lambda x: reader(x))
    total = records.map(lambda x: ((x[0], "Total"), (x[1], x[2])))
    bx = records.map(lambda x: ((x[0], "Bx"), (x[3], x[4])))
    bk = records.map(lambda x: ((x[0], "Bk"), (x[5], x[6])))
    mn = records.map(lambda x: ((x[0], "M"), (x[7], x[8])))
    qn = records.map(lambda x: ((x[0], "Q"), (x[9], x[10])))
    si = records.map(lambda x: ((x[0], "SI"), (x[11], x[12])))

    turnstile = sc.textFile(sys.argv[2], 1)
    turnstile = turnstile.mapPartitions(lambda x: reader(x))

    bx_t = turnstile.filter(lambda x:x[1] == "Bx").map(lambda x: ((x[0], x[1]), (x[2], x[3])))
    bk_t = turnstile.filter(lambda x:x[1] == "Bk").map(lambda x: ((x[0], x[1]), (x[2], x[3])))
    mn_t = turnstile.filter(lambda x:x[1] == "M").map(lambda x: ((x[0], x[1]), (x[2], x[3])))
    qn_t = turnstile.filter(lambda x:x[1] == "Q").map(lambda x: ((x[0], x[1]), (x[2], x[3])))
    si_t = turnstile.filter(lambda x:x[1] == "SI").map(lambda x: ((x[0], x[1]), (x[2], x[3])))

    turnstile_total = sc.textFile(sys.argv[3], 1)
    turnstile_total = turnstile_total.mapPartitions(lambda x: reader(x))
    total_t = turnstile_total.map(lambda x: ((x[0], "Total"), (x[1], x[2])))

    bx_combined = bx.join(bx_t)
    bk_combined = bk.join(bk_t)
    mn_combined = mn.join(mn_t)
    qn_combined = qn.join(qn_t)
    si_combined = si.join(si_t)
    total_combined = total.join(total_t)

    result = bx_combined+bk_combined+mn_combined+qn_combined+si_combined+total_combined
    result = result.map(lambda x:(x[0][0], x[0][1], x[1][0][0], x[1][0][1], x[1][1][0], x[1][1][1]))
    result = result.sortBy(lambda x: (x[0], x[1])).map(lambda x: ",".join(x))
    result.saveAsTextFile("task1c.out")
    sc.stop


