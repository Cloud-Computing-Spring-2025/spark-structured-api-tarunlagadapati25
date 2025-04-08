from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Task 4 - Happy Recommendations").getOrCreate()

# Load input data
logs_df = spark.read.option("header", True).csv("input/listening_logs.csv")
songs_df = spark.read.option("header", True).csv("input/songs_metadata.csv")

# Join logs with song metadata
logs_enriched = logs_df.join(songs_df, on="song_id")

# Count mood plays per user
mood_counts = logs_enriched.groupBy("user_id", "mood").agg(count("*").alias("count"))
window_spec = Window.partitionBy("user_id").orderBy(col("count").desc())
ranked_moods = mood_counts.withColumn("rank", row_number().over(window_spec))

# Select users who mostly listen to "Sad" songs
sad_pref_users = ranked_moods.filter((col("rank") == 1) & (col("mood") == "Sad")).select("user_id")

# Songs each user has already listened to
user_song_df = logs_df.select("user_id", "song_id").distinct()

# Get Happy songs from metadata
happy_songs = songs_df.filter(col("mood") == "Happy")

# Cross join Sad users with Happy songs
recommend_pool = sad_pref_users.crossJoin(happy_songs)

# Eliminate songs already played
recommendations = recommend_pool.join(user_song_df, on=["user_id", "song_id"], how="left_anti")

# Limit to 3 recommendations per user
window_spec_reco = Window.partitionBy("user_id").orderBy("song_id")
top_recs = recommendations.withColumn("rank", row_number().over(window_spec_reco)) \
                          .filter(col("rank") <= 3).drop("rank")

# Save result
top_recs.write.format("csv").option("header", True).mode("overwrite").save("output/happy_recommendations/")
