from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Task 1 - User Favorite Genres").getOrCreate()

# Load input data
logs_df = spark.read.option("header", True).csv("input/listening_logs.csv")
songs_df = spark.read.option("header", True).csv("input/songs_metadata.csv")

# Join logs with song metadata
joined_df = logs_df.join(songs_df, on="song_id")

# Count plays by user and genre
genre_counts = joined_df.groupBy("user_id", "genre").agg(count("*").alias("play_count"))

# Rank genres per user
window_spec = Window.partitionBy("user_id").orderBy(col("play_count").desc())
ranked = genre_counts.withColumn("rank", row_number().over(window_spec))

# Keep top genre per user
favorite_genres = ranked.filter(col("rank") == 1).drop("rank")

# Save output
favorite_genres.write.format("csv").option("header", True).mode("overwrite").save("output/user_favorite_genres/")
