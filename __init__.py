from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
#val spark = SparkSession.builder().master("local[1]").appName("Test").getOrCreate()
data = spark.read.option("header",True).option("inferSchema",True).option("delimiter", ",").csv("/E:/Practise_Code/test/RawData.txt")
#val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/E:/Practise_Code/test/RawData.txt")
data.show()
#df.show()
data.write.mode("overwrite").parquet("/E:/Practise_Code/test/FormatedData.parquet")
#df.write.mode("overwrite").parquet("/E:/Practise_Code/test/FormatedData.parquet")