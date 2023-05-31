from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, unix_timestamp, when, isnull
from pyspark.sql import functions as F
import string, random

def generate_sessionIDs(data):
    sessionID = ""
    updatedData = []
    totalSessionTime = 0
    for x in data:
        a = x["diff"]
        if (a >= 1800) | (totalSessionTime >= 7200):
            alphanumeric = string.ascii_letters + string.digits
            totalSessionTime = 0
            sessionID = x["userid"] + "_" + "".join(random.choices(alphanumeric, k=10))
            updatedData.append([x.userid,x.timestamp,sessionID,x.diff])
        elif (a < 1800) & (totalSessionTime <= 7200):
            totalSessionTime += a
            updatedData.append([x.userid,x.timestamp,sessionID,x.diff])
        elif a == 0 :
            updatedData.append([x.userid,x.timestamp,sessionID,x.diff])
        else:
            totalSessionTime = 0
            sessionID = x["userid"] + "_" + "".join(random.choices(alphanumeric, k=10))
            updatedData.append([x.userid,x.timestamp,sessionID,x.diff])
    return updatedData       
            
spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
raw_data = spark.read.option("header",True).option("inferSchema",True).option("delimiter", ",").csv("RawData.txt")
w = Window.partitionBy("userid").orderBy("timestamp")
dif = raw_data.withColumn("diff", unix_timestamp(col("timestamp")) - unix_timestamp(F.lag("timestamp").over(w)))
dif = dif.withColumn("diff", when(isnull(col("diff")), 1801).otherwise(col("diff")))
dif = dif.orderBy("timestamp").repartition("userid")
sessions = dif.rdd.mapPartitions(generate_sessionIDs).toDF(["UserID","Timestamp","SessionID","diff"])
sessions.write.mode("overwrite").option("header", True).option("inferSchema", True).parquet("FormatedData.parquet")



data = spark.read.parquet("FormatedData.parquet")  
data.write.option("header", True).mode("overwrite").csv("table.csv")
enriched_data = spark.read.option("header", True).csv("table.csv")
enriched_data.createOrReplaceTempView("New_Session_Data")
#spark.sql("""SELECT count(*) AS count FROM New_Session_Data""").show()
#spark.sql("""SELECT count(*) AS SessionsCount, SessionID FROM New_Session_Data GROUP BY SessionID ORDER BY SessionsCount""").show()
spark.sql("""SELECT count(DISTINCT SessionID) AS SessionsCount, date_trunc('day',Timestamp) AS DATE FROM New_Session_Data
                 GROUP BY DATE ORDER BY DATE""").show()
spark.sql("""SELECT UserID, SUM(TimeSpent) as TotalTimeSpent_DAY, DATE as TimeStamp FROM
            (SELECT UserID, FROM_UNIXTIME(SUM(CASE WHEN diff < 1800 THEN diff ELSE 1800 END), 'HH') AS TimeSpent, date_trunc('day',Timestamp) AS DATE 
            FROM New_Session_Data GROUP BY DATE, SessionID, UserID)
            GROUP BY DATE, UserID ORDER BY UserID, TimeStamp""").show()
spark.sql("""SELECT UserID, SUM(TimeSpent) as TotalTimeSpent_DAY, DATE as TimeStamp FROM
            (SELECT UserID, FROM_UNIXTIME(SUM(SELECT diff FROM New_Session_Data GROUP BY SessionID), 'HH') AS TimeSpent, date_trunc('day',Timestamp) AS DATE 
            FROM New_Session_Data GROUP BY DATE, SessionID, UserID)
            GROUP BY DATE, UserID ORDER BY UserID, TimeStamp""").show()
spark.sql("""SELECT UserID, SUM(TimeSpent) as TotalTimeSpent_MONTH, MONTH as TimeStamp FROM
            (SELECT UserID, FROM_UNIXTIME(SUM(CASE WHEN diff < 1800 THEN diff ELSE 1800 END), 'HH') AS TimeSpent, date_trunc('month',Timestamp) AS MONTH
            FROM New_Session_Data GROUP BY MONTH, SessionID, UserID)
            GROUP BY MONTH, UserID ORDER BY UserID, TimeStamp""").show()