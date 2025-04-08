from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, weekofyear, year, desc

# Initialize Spark session
spark = SparkSession.builder.appName("Task 3 - Top Songs This Week").getOrCreate()

# Load data
logs_df = spark.read.option("header", True).csv("input/listening_logs.csv")
songs_df = spark.read.option("header", True).csv("input/songs_metadata.csv")

# Convert timestamp
logs_df = logs_df.withColumn("timestamp", to_timestamp("timestamp"))

# Extract week and year
logs_df = logs_df.withColumn("week", weekofyear(col("timestamp"))) \
                 .withColumn("year", year(col("timestamp")))

# Get the latest week and year
current_week = logs_df.select("week", "year").orderBy(col("year").desc(), col("week").desc()).first()
week_val = current_week["week"]
year_val = current_week["year"]

# Filter logs for current week
this_week_logs = logs_df.filter((col("week") == week_val) & (col("year") == year_val))

# Count plays per song
top_songs = this_week_logs.groupBy("song_id").count().orderBy(desc("count")).limit(10)

# Join with song metadata for better context
result = top_songs.join(songs_df, on="song_id", how="left") \
                  .select("song_id", "title", "artist", "count")

# Save result
result.write.format("csv").option("header", True).mode("overwrite").save("output/top_songs_this_week/")
