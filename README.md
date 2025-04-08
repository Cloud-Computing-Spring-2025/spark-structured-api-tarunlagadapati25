# 🎵 Assignment 3 - Spark Structured API: Music Listener Behavior Analysis

## Overview
This Assignment involves analyzing music listener behavior using Apache Spark Structured APIs with PySpark. We simulate a music streaming platform by generating synthetic datasets and perform various data analysis tasks such as identifying favorite genres, calculating average listening durations, recommending songs, and more.

The analysis is executed using PySpark scripts and results are saved into structured output folders for each task.

---

## Prerequisites & Setup

### Tools & Libraries Used
- **Python 3.x**
- **PySpark**: For distributed data processing
- **Faker**: To generate realistic user names and song metadata
- **pandas**: For inspecting data during development

### Setup in GitHub Codespaces
We used GitHub Codespaces for development. The environment was configured with:
- `pip install pyspark faker`

### Dataset Generation
We used a script `generate_dataset.py` that creates two CSV files in the `input/` folder:

#### `songs_metadata.csv`
- `song_id`: Unique identifier for a song
- `title`: Randomly generated song title
- `artist`: Artist name from Faker
- `genre`: One of ['Pop', 'Rock', 'Jazz', 'Hip-Hop', 'Classical']
- `mood`: One of ['Happy', 'Sad', 'Energetic', 'Chill']

#### `listening_logs.csv`
- `user_id`: User who played the song
- `song_id`: ID of the song played
- `timestamp`: Time of play
- `duration_sec`: Duration of the session in seconds

#### Genre Biasing
To make genre loyalty scores meaningful, we introduced user-specific genre preferences so that ~80% of their listening logs came from their preferred genre.

---

## Folder Structure
```
assignment-3-spark-structured-api-Scheruk1701/
├── input/
│   ├── songs_metadata.csv
│   └── listening_logs.csv
├── output/
│   ├── user_favorite_genres/
│   ├── avg_listen_time_per_song/
│   ├── top_songs_this_week/
│   ├── happy_recommendations/
│   ├── genre_loyalty_scores/
│   └── night_owl_users/
├── screenshots/   # Screenshots of results 
├── src/
│   ├── task1_user_favorite_genres.py
│   ├── task2_avg_listen_time_per_song.py
│   ├── task3_top_songs_this_week.py
│   ├── task4_happy_recommendations.py
│   ├── task5_genre_loyalty_scores.py
│   └── task6_night_owl_users.py
├── generate_dataset.py
└── README.md
```

---

## Task Breakdown with Code Explanation

### Task 1: Find each user’s favorite genre.
**Goal:** Identify each user’s most listened-to genre.

**Logic:**
1. Join `listening_logs` with `songs_metadata` on `song_id`
2. Group by `user_id` and `genre`, count plays
3. Use a window function to rank genres per user
4. Select the top-ranked genre as the favorite

```bash
spark-submit src/task1_user_favorite_genres.py
```
**Output Folder:** `output/user_favorite_genres/`

**Sample Output:**
```
user_id,genre,play_count
user_0001,Pop,11
user_0002,Pop,8
```

---

### Task 2: Calculate the average listen time per song.
**Goal:** Compute average duration (in seconds) for each song.

**Logic:**
1. Cast `duration_sec` to integer
2. Group by `song_id` and calculate average
3. Use `round()` to keep 2 decimal places

```bash
spark-submit src/task2_avg_listen_time_per_song.py
```
**Output Folder:** `output/avg_listen_time_per_song/`

**Sample Output:**
```
song_id,avg_listen_time_sec
song_0090,172.67
song_0115,182.8
```

---

### Task 3: List the top 10 most played songs this week.
**Goal:** Identify top 10 most played songs in the current week.

**Logic:**
1. Extract week number and year from timestamp
2. Filter logs for the current week
3. Group by `song_id` and count plays
4. Join with metadata for title & artist info

```bash
spark-submit src/task3_top_songs_this_week.py
```
**Output Folder:** `output/top_songs_this_week/`

**Sample Output:**
```
song_id,title,artist,count
song_0123,Rise arm story,Sarah Klein DDS,5
song_0046,Hair other put threat,David Cunningham,5
```

---

### Task 4: Recommend “Happy” songs to users who mostly listen to “Sad” songs.
**Goal:** Recommend up to 3 "Happy" songs to users who mainly listen to "Sad" songs.

**Logic:**
1. Identify users whose top mood is "Sad"
2. Select all "Happy" songs from catalog
3. Exclude songs the user has already played
4. Recommend up to 3 using window ranking

```bash
spark-submit src/task4_happy_recommendations.py
```
**Output Folder:** `output/happy_recommendations/`

**Sample Output:**
```
user_id,song_id,title,artist,genre,mood
user_0001,song_0004,Above stay our,Andrew Frye,Pop,Happy
user_0001,song_0006,Only white special,Jennifer Jones,Pop,Happy
user_0001,song_0008,Collection everybody we,Michael Vasquez,Rock,Happy
```

---

### Task 5: Compute the genre loyalty score for each user.
**Goal:** Calculate the proportion of plays that belong to each user’s most-listened genre.

**Logic:**
1. Count plays per user and genre
2. Calculate total plays per user
3. Compute loyalty score = top genre plays / total
4. Filter users with loyalty score > 0.8

```bash
spark-submit src/task5_genre_loyalty_scores.py
```
**Output Folder:** `output/genre_loyalty_scores/`

**Sample Output:**
```
user_id,genre,loyalty_score
user_0001,Pop,0.92
user_0002,Pop,1.0
```

---

### Task 6: Identify users who listen to music between 12 AM and 5 AM.
**Goal:** Identify users who frequently listen to music between 12 AM – 5 AM.

**Logic:**
1. Extract hour from timestamp
2. Filter rows between 0 and 5 hours
3. Count distinct night-time plays per user
4. Filter users with more than 5 sessions

```bash
spark-submit src/task6_night_owl_users.py
```
**Output Folder:** `output/night_owl_users/`

**Sample Output:**
```
user_id,night_play_count
user_0025,6
user_0071,6
```

---

## Errors & Fixes
| Error | Cause | Solution |
|-------|-------|----------|
| `NameError: 'row_number' not defined` | Forgot to import function | Added `from pyspark.sql.functions import row_number` |
| Empty loyalty score output | Dataset too randomized | Biased user listening with preferred genre |
| Timestamp issues | Wrong format or string type | Used `to_timestamp()` to convert |

---

