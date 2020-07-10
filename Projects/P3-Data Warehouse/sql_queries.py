import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES


staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id                BIGINT identity(0, 1),
    artist                  VARCHAR,
    auth                    VARCHAR,
    firstName               VARCHAR,
    gender                  VARCHAR,
    itemInSession           VARCHAR,
    lastName                VARCHAR,
    length                  VARCHAR,
    level                   VARCHAR,
    location                VARCHAR,
    method                  VARCHAR,
    page                    VARCHAR,
    registration            VARCHAR,
    sessionId               INT SORTKEY DISTKEY,
    song                    TEXT,
    status                  INT,
    ts                      BIGINT,
    userAgent               TEXT,
    userId                  INT
);
""")


staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id             TEXT,
    num_songs           INTEGER,
    artist_id           TEXT SORTKEY DISTKEY,
    artist_latitude      DOUBLE PRECISION,
    artist_longitude     DOUBLE PRECISION,
    artist_location      TEXT,
    artist_name          TEXT,
    title                TEXT,
    duration             FLOAT,
    year                 INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id         BIGINT identity(0, 1) SORTKEY,
    start_time          timestamp,
    user_id             TEXT DISTKEY,
    level               TEXT,
    song_id             VARCHAR,
    artist_id           VARCHAR,
    session_id          TEXT,
    location            TEXT,
    user_agent          TEXT
);
""")


user_table_create = ("""
    CREATE TABLE users(
        user_id             INTEGER  SORTKEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              VARCHAR,
        level               VARCHAR 
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id             VARCHAR  NOT NULL SORTKEY,
        title               VARCHAR  NOT NULL,
        artist_id           VARCHAR  NOT NULL,
        year                INTEGER  NOT NULL,
        duration            FLOAT NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id           VARCHAR         SORTKEY,
        name                VARCHAR,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time          TIMESTAMP       NOT NULL DISTKEY SORTKEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
    ) diststyle all;
""")

#STAGING DATA

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    STATUPDATE ON
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'],
            role_arn=config['IAM_ROLE']['ARN'],
            log_json_path=config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'],
            role_arn=config['IAM_ROLE']['ARN'])

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
        e.userId    AS user_id,
        e.level     AS level,
        s.song_id   AS song_id,
        s.artist_id AS artist_id,
        e.sessionId AS session_id,
        e.location  AS location,
        e.userAgent AS user_agent
    FROM staging_events AS e
    JOIN staging_songs AS s
        ON (e.artist = s.artist_name)
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT userId    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  as weekday
    FROM staging_events
    WHERE page = 'NextSong';
""")





# GET NUMBER OF ROWS IN EACH TABLE
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_time = ("""
    SELECT COUNT(*) FROM time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_number_rows_queries= [get_number_staging_events, get_number_staging_songs, get_number_songplays, get_number_users, get_number_songs, get_number_artists, get_number_time]
