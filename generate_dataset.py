import csv
import random
from datetime import datetime, timedelta
import os
from faker import Faker
from collections import defaultdict

fake = Faker()
random.seed(42)

# --- Parameters ---
NUM_USERS = 100
NUM_SONGS = 200
NUM_LOGS = 1000

GENRES = ['Pop', 'Rock', 'Jazz', 'Hip-Hop', 'Classical']
MOODS = ['Happy', 'Sad', 'Energetic', 'Chill']

# Ensure the output directory exists
os.makedirs("input", exist_ok=True)

# --- 1. Generate users with names ---
user_ids = [f"user_{i:04d}" for i in range(1, NUM_USERS + 1)]

# --- 2. Generate songs_metadata.csv with better names ---
songs = []
song_ids = []
for i in range(1, NUM_SONGS + 1):
    song_id = f"song_{i:04d}"
    song_ids.append(song_id)
    title = fake.sentence(nb_words=3).replace('.', '')
    artist = fake.name()
    genre = random.choice(GENRES)
    mood = random.choice(MOODS)
    songs.append([song_id, title, artist, genre, mood])

with open("input/songs_metadata.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["song_id", "title", "artist", "genre", "mood"])
    writer.writerows(songs)

# --- 3. Generate listening_logs.csv with genre preference bias ---
user_genre_pref = {user_id: random.choice(GENRES) for user_id in user_ids}

# Index songs by genre
songs_by_genre = defaultdict(list)
for song in songs:
    song_id, _, _, genre, _ = song
    songs_by_genre[genre].append(song_id)

logs = []
for _ in range(NUM_LOGS):
    user_id = random.choice(user_ids)
    preferred_genre = user_genre_pref[user_id]

    # 80% chance from preferred genre
    if random.random() < 0.8:
        song_id = random.choice(songs_by_genre[preferred_genre])
    else:
        song_id = random.choice(song_ids)

    timestamp = datetime.now() - timedelta(days=random.randint(0, 30),
                                           minutes=random.randint(0, 1440))
    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    duration_sec = random.randint(30, 300)
    logs.append([user_id, song_id, timestamp_str, duration_sec])

with open("input/listening_logs.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["user_id", "song_id", "timestamp", "duration_sec"])
    writer.writerows(logs)

print("Generated input datasets")
