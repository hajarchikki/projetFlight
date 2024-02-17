from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col, desc, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Flight Delays").getOrCreate()

df = spark.read.option('header', 'true').csv('flights.csv')
#df.show()

df.show(10)
df.printSchema()

df_with_delay= df.withColumn("is_delayed", when(col("ARRIVAL_DELAY") > 15, 1).otherwise(0))

df_no = df_with_delay.na.drop(subset=["ARRIVAL_DELAY", "DEPARTURE_DELAY"])

avg_delay_by_airline = df_no.groupBy("AIRLINE").agg(avg("ARRIVAL_DELAY").alias("avg_delay"))
avg_delay_by_airport = df_no.groupBy("ORIGIN_AIRPORT").agg(avg("ARRIVAL_DELAY").alias("avg_delay"))

top_delayed_flights = df_no.orderBy(desc("ARRIVAL_DELAY")).limit(10)
avg_delay_by_airline.show()
avg_delay_by_airport.show()
top_delayed_flights.show()

airport_counts = df.groupBy("ORIGIN_AIRPORT").count()
window = Window.orderBy(desc("count"))
ranked_airports = airport_counts.withColumn("rank", rank().over(window))

ranked_airports.show()


