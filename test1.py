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
        elif (a < 1800) & (totalSessionTime < 7200):
            totalSessionTime += a
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

#get_session_id = F.udf(calculate_result, IntegerType())
#x = 0
#sessions = dif.withColumn("sessionID", when((col("diff") >= 1800), concat(col("userid"), lit("_"), substring(rand()*1000000, 1, 10)))
                                        #.when(col("diff") < 1800, x+1)
                                        #.when(isnull(col("diff")), concat(col("userid"), lit("_"), substring(rand()*1000000, 1, 10))))
                                        #.otherwise(generate_sessionIDs(col("userid"), False)))
                                        #TODO: Add condition if total diff > 2hrs
                                        #.when(dif.group))
                
sessions = dif.rdd.mapPartitions(generate_sessionIDs).toDF(["UserID","Timestamp","SessionID","diff"])
#sessions.show()

# session_IDs = sessions.withColumn("sessionID", when(col("sessionID") == "asAbove", lag("sessionID").over(w)).otherwise(col("sessionID")))
# grouped = session_IDs.groupBy("sessionID").agg(sum("diff").alias("TotalTimeOfSession"))
# session_IDs = session_IDs.join(grouped, "sessionID", "left")
# session_IDs.show()

sessions.write.mode("overwrite").parquet("FormatedData.parquet")

data = spark.read.parquet("FormatedData.parquet")
data.createOrReplaceTempView("Session_Data")
spark.sql("""SELECT count(*) AS count FROM Session_Data""").show()
spark.sql("""SELECT count(*) AS SessionsCount, SessionID FROM Session_Data GROUP BY SessionID""").show()
spark.sql("""SELECT UserID, SUM(TimeSpent) as TotalTimeSpent_DAY, DATE as TimeStamp FROM
            (SELECT UserID, FROM_UNIXTIME(SUM(CASE WHEN diff < 1800 THEN diff END), 'HH') AS TimeSpent, date_trunc('day',Timestamp) AS DATE 
            FROM Session_Data GROUP BY DATE, SessionID, UserID)
            GROUP BY DATE, UserID""").show()
spark.sql("""SELECT UserID, SUM(TimeSpent) as TotalTimeSpent_MONTH, MONTH as TimeStamp FROM
            (SELECT UserID, FROM_UNIXTIME(SUM(CASE WHEN diff < 1800 THEN diff END), 'HH') AS TimeSpent, date_trunc('month',Timestamp) AS MONTH
            FROM Session_Data GROUP BY MONTH, SessionID, UserID)
            GROUP BY MONTH, UserID""").show()