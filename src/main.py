from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col, desc

spark = SparkSession.builder.appName("Flight Delays").getOrCreate()

df = spark.read.csv("flights.csv", header=True, inferSchema=True)
#df.printSchema()

df.show(10)
df.printSchema()

df = df.withColumn("is_delayed", when(col("ARRIVAL_DELAY") > 15, 1).otherwise(0))

df = df.na.drop(subset=["ARRIVAL_DELAY", "DEPARTURE_DELAY"])

avg_delay_by_airline = df.groupBy("AIRLINE").agg(avg("ARRIVAL_DELAY").alias("avg_delay"))
avg_delay_by_airport = df.groupBy("ORIGIN_AIRPORT").agg(avg("ARRIVAL_DELAY").alias("avg_delay"))

top_delayed_flights = df.orderBy(desc("ARRIVAL_DELAY")).limit(10)

