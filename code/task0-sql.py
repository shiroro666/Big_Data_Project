from csv import reader
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as f
from pyspark.sql.functions import format_string

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
    cases = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
    cases.createOrReplaceTempView("cases")
    result = spark.sql("SELECT date_of_interest, case_count, SUM(case_count) OVER (ORDER BY date_of_interest) as cumulative_total, bx_case_count, SUM(bx_case_count) OVER (ORDER BY date_of_interest) as bx_cumulative_total, bk_case_count, SUM(bk_case_count) OVER (ORDER BY date_of_interest) as bk_cumulative_total, mn_case_count, SUM(mn_case_count) OVER (ORDER BY date_of_interest) as mn_cumulative_total, qn_case_count, SUM(qn_case_count) OVER (ORDER BY date_of_interest) as qn_cumulative_total, si_case_count, SUM(si_case_count) OVER (ORDER BY date_of_interest) as si_cumulative_total FROM cases")
    #result = result.withColumn('pickup_datetime', f.from_unixtime(f.unix_timestamp(trips.pickup_datetime), "yyyy-MM-dd HH:mm:ss"))
    result.select(format_string('%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d',result.date_of_interest, result.case_count, result.cumulative_total, result.bx_case_count, result.bx_cumulative_total, result.bk_case_count, result.bk_cumulative_total, result.mn_case_count, result.mn_cumulative_total, result.qn_case_count, result.qn_cumulative_total, result.si_case_count, result.si_cumulative_total)).write.save('task0-sql.out',format="text")

