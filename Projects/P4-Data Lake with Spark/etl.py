import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear
from pyspark.sql import types as t

config = configparser.ConfigParser()
config.read('dl.cfg')

# Configure AWS
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    # Setting a Apache Spark session to process the data.

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    start_sd = datetime.now()
    start_sdl = datetime.now()
    print("Start processing song_data JSON files...")

    # get filepath to song data file
    song_data = input_data

    # read song data file
    print("Reading song_data files from {}...".format(song_data))
    df_sd = spark.read.json(song_data)

    print("...finished reading song_data ")
    print("Song_data schema:")
    df_sd.printSchema()

    df_sd.createOrReplaceTempView("songs_table_DF")

    # Create Song_Table
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM songs_table_DF
        ORDER BY song_id
    """)
    print("Songs_table schema:")
    songs_table.printSchema()
    print("Songs_table examples:")
    songs_table.show(5, truncate=False)

    # write songs table to parquet files
    songs_table_path = output_data + "songs_table.parquet"

    # Write DF to Spark parquet file partitioned by year and artist_id
    print("Writing songs_table parquet files to {}...".format(songs_table_path))
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(songs_table_path)

    print("...finished writing songs_table")

    # create artists table
    df_sd.createOrReplaceTempView("artists_table_DF")
    artists_table = spark.sql("""
        SELECT  artist_id        AS artist_id,
                artist_name      AS name,
                artist_location  AS location,
                artist_latitude  AS latitude,
                artist_longitude AS longitude
        FROM artists_table_DF
        ORDER BY artist_id desc
    """)
    artists_table.printSchema()
    artists_table.show(5, truncate=False)

    # write artists table to parquet files
    artists_table_path = output_data + "artists_table.parquet"
    print("Writing artists_table parquet files to {}...".format(artists_table_path))
    songs_table.write.mode("overwrite").parquet(artists_table_path)

    print("...finished writing artists_table")
    print("Finished processing song_data")

    return songs_table, artists_table


def process_log_data(spark, input_data_ld, input_data_sd, output_data):

    print("Start processing log_data JSON files...")
    # get filepath to log data file
    log_data = input_data_ld

    # read log data file
    print("Reading log_data files from {}...".format(log_data))
    df_ld = spark.read.json(log_data)

    print("...finished reading log_data")

    df_ld_filtered = df_ld.filter(df_ld.page == 'NextSong')

    # USERS TABLE
    # extract columns for users table
    df_ld_filtered.createOrReplaceTempView("users_table_DF")
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id,
                         firstName AS first_name,
                         lastName  AS last_name,
                         gender,
                         level
        FROM users_table_DF
        ORDER BY last_name
    """)
    print("Users_table schema:")
    users_table.printSchema()
    print("Users_table examples:")
    users_table.show(5)

    # write users table to parquet files
    users_table_path = output_data + "users_table.parquet"
    print("Writing users_table parquet files to {}...".format(users_table_path))
    users_table.write.mode("overwrite").parquet(users_table_path)

    print("...finished writing users_table")

    # create timestamp column from original timestamp column
    print("Creating timestamp column...")

    @udf(t.TimestampType())
    def get_timestamp(ts):
        return datetime.fromtimestamp(ts / 1000.0)

    df_ld_filtered = df_ld_filtered.withColumn("timestamp", get_timestamp("ts"))
    df_ld_filtered.printSchema()
    df_ld_filtered.show(5)

    # create datetime column from original timestamp column
    print("Creating datetime column...")

    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0) \
            .strftime('%Y-%m-%d %H:%M:%S')

    df_ld_filtered = df_ld_filtered.withColumn("datetime", \
                                               get_datetime("ts"))
    print("Log_data + timestamp + datetime columns schema:")
    df_ld_filtered.printSchema()
    print("Log_data + timestamp + datetime columns examples:")
    df_ld_filtered.show(5)

    # extract columns to create time table
    df_ld_filtered.createOrReplaceTempView("time_table_DF")
    time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time,
                         hour(timestamp) AS hour,
                         day(timestamp)  AS day,
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM time_table_DF
        ORDER BY start_time
    """)
    print("Time_table schema:")
    time_table.printSchema()
    print("Time_table examples:")
    time_table.show(5)

    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + "time_table.parquet"
    print("Writing time_table parquet files to {}..." \
          .format(time_table_path))
    time_table.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(time_table_path)
    print("...finished writing time_table")

    song_data = input_data_sd
    print("Reading song_data files from {}...".format(song_data))
    df_sd = spark.read.json(song_data)

    # Join log_data and song_data DFs
    print("Joining log_data and song_data DFs...")
    df_ld_sd_joined = df_ld_filtered \
        .join(df_sd, (df_ld_filtered.artist == df_sd.artist_name) & \
              (df_ld_filtered.song == df_sd.title))
    print("...finished joining song_data and log_data DFs.")
    print("Joined song_data + log_data schema:")
    df_ld_sd_joined.printSchema()
    print("Joined song_data + log_data examples:")
    df_ld_sd_joined.show(5)

    # extract columns from joined song and log datasets
    # to create songplays table
    print("Extracting columns from joined DF...")
    df_ld_sd_joined = df_ld_sd_joined.withColumn("songplay_id", monotonically_increasing_id())
    df_ld_sd_joined.createOrReplaceTempView("songplays_table_DF")
    songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id,
                timestamp   AS start_time,
                userId      AS user_id,
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM songplays_table_DF
        ORDER BY (user_id, session_id)
    """)

    print("Songplays_table schema:")
    songplays_table.printSchema()
    print("Songplays_table examples:")
    songplays_table.show(5, truncate=False)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + "songplays_table.parquet"

    print("Writing songplays_table parquet files to {}..." \
          .format(songplays_table_path))
    time_table.write.mode("overwrite").partitionBy("year", "month") \
        .parquet(songplays_table_path)

    print("...finished writing songplays_table")

    return users_table, time_table, songplays_table


def main():
    spark = create_spark_session()

    # Use these input and output path to load and store data from S3.
    #     input_songs_data = config['AWS']['SONGS_DATA']
    #     input_logs_data = config['AWS']['LOGS_DATA']
    #     output_data = config['AWS']['OUTPUT']

    # Access data from LOCAL Storage.
    input_songs_data = config['LOCAL']['SONGS_DATA']
    input_logs_data = config['LOCAL']['LOGS_DATA']
    output_data = config['LOCAL']['OUTPUT']

    songs_table, artists_table = process_song_data(spark, input_songs_data, output_data)
    users_table, time_table, songplays_table = process_log_data(spark, input_logs_data, input_songs_data, output_data)

    print("DONE PROCESSING !!!!!!")


if __name__ == "__main__":
    main()