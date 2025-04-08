from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("Task 6 - Night Owl Users").getOrCreate()

# Load logs
logs_df = spark.read.option("header", True).csv("input/listening_logs.csv")

# Convert timestamp to proper type
logs_df = logs_df.withColumn("timestamp", to_timestamp("timestamp"))

# Extract hour of day from timestamp
logs_df = logs_df.withColumn("hour", hour(col("timestamp")))

# Filter for logs between 12 AM and 5 AM
night_logs = logs_df.filter((col("hour") >= 0) & (col("hour") <= 5))

# Count distinct night-time plays per user
night_owls = night_logs.groupBy("user_id").agg(countDistinct("timestamp").alias("night_play_count"))

# Optional: Only show users with more than 5 plays at night
night_owls_filtered = night_owls.filter(col("night_play_count") > 5)

# Save output
night_owls_filtered.write.format("csv").option("header", True).mode("overwrite").save("output/night_owl_users/")
