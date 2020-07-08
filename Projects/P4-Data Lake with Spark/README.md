# Data Lake for Sparkify

Goal: Building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

The ELT is built with Python. The data is pulled from S3 buckets, transformed and loaded with PySpark. Testing the Spark ELT on a small subset of the data was done locally in Spark Local while a Spark cluster with AWS ElasticMapReduce was used for the complete dataset. All data was saved to an S3 bucket in parquet. 

This documentation contains:

1. Table design and schemas
2. Data extraction and transformation
3. Explanation of files in the repository
4. Running the scripts

## 1. Table Design and Schemas

### Fact Table

1. songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

2. users - users in the app
- user_id, first_name, last_name, gender, level
3. songs - songs in music database
- song_id, title, artist_id, year, duration
4. artists - artists in music database
- artist_id, name, location, lattitude, longitude
5. time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

## 2. Data extraction and transformation

1. Load AWS credentials  (dl.cfg)
2. Read Data from S3 Buckets
- Bucket 1 (song_data): s3://udacity-dend/song_data 
    - contains static data about artists and songs
- Bucket 2 (log_data): s3://udacity-dend/log_data 
    - contains user data (who listened what song, when, where, etc)
* Data from the Million Songs dataset that consists of song and artist data
* Logs generated with this dataset and the event generator
3. Process the data using Apache Spark.
4. Load back the data onto S3.

## 3. Explanation of files in the repository


* `dl.cfg`: File contains the requred AWS Credentials and Input and Output paths of the Data.

* `etl.py`: Use Spark to extract, load and transform the big song and log dataset on Udacity's S3 bucket and save it as parquet to an output path, also on a S3 bucket.

## 4. Running the scripts

To run the scripts:

```
python etl.py
```

#### Note: 
1. Unzip the `Data` folder before running the `etl.py` file.
2. It is not recomended to run the program locally with huge dataset since it's going to take extremely large amount of time to process. 

----