# Data Warehouse for Sparkify

The goal of this project is to build a data warehouse for Sparkify:

- To build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. Where we will be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

The ETL is built with Python. The data is pulled from S3 buckets, transformed and loaded into a 4 node `dc2.large` Redshift cluster. An IAM user is defined for this whole process with permissions to read from S3 buckets and access Redshift clusters.

# This documentation Contains:

1. Table design and schemas
2. Data extraction and transformation
3. Explanation of files in the repository
4. Running the scripts

## 1. Table Design and Schemas

## Fact Table
1. songplays - records in event data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
## Dimension Tables
2. users - users in the app
- user_id, first_name, last_name, gender, level
3. songs - songs in music database
- song_id, title, artist_id, year, duration
4. artists - artists in music database
- artist_id, name, location, lattitude, longitude
5. time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

## 2. Data extraction and transformation

## DataSet
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data
- Log data json path: s3://udacity-dend/log_json_path.json

* Data from the Million Songs dataset that consists of song and artist data
* Logs generated with this dataset and the event generator

## Create Table Schemas
1. Design schemas for your fact and dimension tables
2. Write a SQL CREATE statement for each of these tables in `sql_queries.py`
3. Complete the logic in `create_tables.py` to connect to the database and create these tables
4. Write SQL DROP statements to drop tables in the beginning of `create_tables.py` if the tables already exist. This way, you can run `create_tables.py` whenever you want to reset your database and test your ETL pipeline.
5. Launch a redshift cluster and create an IAM role that has read access to S3.
6. Add redshift database and IAM role info to dwh.cfg.
7. Test by running `create_tables.py` and checking the table schemas in your redshift database. You can use Query Editor in the AWS Redshift console for this.
## Build ETL Pipeline
1. Implement the logic in `etl.py` to load data from S3 to staging tables on Redshift.
2. Implement the logic in `etl.py` to load data from staging tables to analytics tables on Redshift.
3. Test by running `etl.py` after running `create_tables.py` and running the analytic queries on your Redshift database to compare your results with the expected results.
4. Delete your redshift cluster when finished.

#### After loading into our staging tables, we use standard INSERT queries to add data to our fact and dimension tables. 


## 3. Explanation of files in the repository

* `create_tables.py`: is where you'll create your fact and dimension tables for the star schema in Redshift.
* `etl.py`: is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* `sql_queries.py`: is where you'll define you SQL statements, which will be imported into the two other files above.
* `dwh.cfg`: contains the credntials to AWS and link to input resources.

## 4. Running the scripts

To run the scripts:

```
python create_tables.py
python etl.py
```

## Resource: Udacity 