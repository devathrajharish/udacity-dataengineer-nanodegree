# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level TEXT,
    song_id varchar(26),
    artist_id varchar(26),
    session_id INT,
    location TEXT,
    user_agent TEXT
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY, 
    first_name TEXT,
    last_name TEXT,
    gender varchar(1),
    level TEXT
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(100),
    artist_id VARCHAR(20) NOT NULL,
    year INTEGER,
    duration FLOAT(5)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR(25) ,
    name TEXT,
    location TEXT,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time BIGINT PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent) \
    values (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level) \
    values (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE \
    SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration) \
    values (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude) \
    values (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday) \
    values (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT song_id, s.artist_id
    FROM songs s
    JOIN artists a ON s.artist_id = a.artist_id
    WHERE title = %s AND name = %s AND duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]