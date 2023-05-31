import csv
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
data = spark.read.parquet("FormatedData.parquet")
data.createOrReplaceTempView("testt")

spark.sql("SELECT userid as UserID, timestamp as Time FROM testt").show()


spark.sql("""
SELECT userid as UserID, sum(unix_timestamp(timestamp)) as TotalTime
FROM testt
GROUP BY userid
""").show()
