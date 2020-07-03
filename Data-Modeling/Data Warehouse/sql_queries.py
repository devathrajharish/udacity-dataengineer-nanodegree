import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DATA
ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
SONGS_JSONPATH = config.get('S3', 'SONGS_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id bigint identity(0, 1) NOT NULL,
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession text,
    lastName text,
    length text,
    level text,
    location text,
    method text,
    page text,
    registration text,
    sessionId int NOT NULL SORTKEY DISTKEY,
    song text,
    status int,
    ts bigint NOT NULL,
    userAgent text,
    userId int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs integer NOT NULL,
    artist_id varchar(20) NOT NULL SORTKEY DISTKEY,
    artist_latitude float,
    artist_longitude float,
    artist_location text,
    artist_name text,
    song_id varchar(20) NOT NULL,
    title text,
    duration float,
    year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id bigint identity(0, 1) NOT NULL SORTKEY,
    start_time timestamp NOT NULL,
    user_id text NOT NULL DISTKEY,
    level text NOT NULL,
    song_id varchar(20) NOT NULL,
    artist_id varchar(20) NOT NULL,
    session_id text NOT NULL,
    location text,
    user_agent text
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int NOT NULL SORTKEY,
    first_name text, 
    last_name text,
    gender text, 
    level text
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar(20) NOT NULL SORTKEY,
    title text NOT NULL,
    artist_id text NOT NULL,
    year int NOT NULL,
    duration float NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar(20) NOT NULL SORTKEY,
    name text,
    location text,
    latitude float,
    longitude float
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp NOT NULL SORTKEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
format as json {}
STATUPDATE ON
region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
format as json 'auto'
ACCEPTINVCHARS AS '^'
STATUPDATE ON
region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
SELECT DISTINCT 
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
    e.userId AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS user_agent
FROM staging_events AS e
JOIN staging_songs AS s
    ON (e.artist = s.artist_name)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT e.userId as user_id,
    e.firstName AS first_name,
    e.lastName AS last_name,
    e.gender AS gender,
    e.level AS level
FROM staging_events AS e
WHERE e.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT s.song_id AS song_id,
    s.title AS title,
    s.artist_id AS artist_id,
    s.year AS year,
    s.duration AS duration
FROM staging_songs AS s;
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT s.artist_id AS artist_id,
    s.artist_name AS name,
    s.artist_location AS location,
    s.artist_latitude AS latitude,
    s.artist_longitude AS longitude
FROM staging_songs AS s;
""")

time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(week FROM start_time) AS weekday
FROM staging_events AS e
WHERE e.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]