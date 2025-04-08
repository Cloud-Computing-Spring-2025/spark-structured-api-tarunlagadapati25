from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# Initialize Spark session
spark = SparkSession.builder.appName("Task 2 - Avg Listen Time Per Song").getOrCreate()

# Load logs
logs_df = spark.read.option("header", True).csv("input/listening_logs.csv")

# Convert duration column to integer
logs_df = logs_df.withColumn("duration_sec", col("duration_sec").cast("int"))

# Calculate average listen time per song
avg_duration_df = logs_df.groupBy("song_id") \
    .agg(round(avg("duration_sec"), 2).alias("avg_listen_time_sec"))

# Save result
avg_duration_df.write.format("csv").option("header", True).mode("overwrite").save("output/avg_listen_time_per_song/")
