from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, round, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Task 5 - Genre Loyalty Scores").getOrCreate()

# Load input data
logs_df = spark.read.option("header", True).csv("input/listening_logs.csv")
songs_df = spark.read.option("header", True).csv("input/songs_metadata.csv")

# Join logs with song metadata
joined_df = logs_df.join(songs_df, on="song_id")

# Count plays by user and genre
genre_counts = joined_df.groupBy("user_id", "genre").agg(count("*").alias("play_count"))

# Total plays per user
total_counts = genre_counts.groupBy("user_id").agg(spark_sum("play_count").alias("total_plays"))

# Join counts with totals
genre_with_total = genre_counts.join(total_counts, on="user_id")
genre_with_total = genre_with_total.withColumn("loyalty_score", col("play_count") / col("total_plays"))

# Get the top genre per user
window_spec = Window.partitionBy("user_id").orderBy(col("loyalty_score").desc())
ranked = genre_with_total.withColumn("rank", row_number().over(window_spec))

# Keep only users with loyalty_score > 0.8
top_loyal = ranked.filter((col("rank") == 1) & (col("loyalty_score") > 0.8)) \
                  .select("user_id", "genre", round("loyalty_score", 2).alias("loyalty_score"))

# Save result
top_loyal.write.format("csv").option("header", True).mode("overwrite").save("output/genre_loyalty_scores/")
