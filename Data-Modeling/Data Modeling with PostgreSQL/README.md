# ETL for Million Songs Dataset for Sparkify
# Documentation contains:
1. Introduction
2. Repository files explained
3. Table Desingn and Schemas
4. How to run the project

# Summary of project
## Introduction:

A Fictional Startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Task is to create a Postgres database with tables designed to optimize queries on song play analysis and create database schema and ETL pipeline for this analysis.

## Repository Files explained


* **[data](data)**: Contains the Data to Perform task on. 
* **[create_tables.py](create_tables.py)**: Connect to Postgres and create, drop tables.
* **[sql_queries.py](sql_queries.py)**: Python script containing SQL-Statements used by create_tables.py and etl.py
* **[etl.py](etl.py)**: Python script to extract the needed information from Data sources and inserting them to the created database schema and tables.
* **[etl.ipynb](etl.ipynb)**: Testing environment to check the dataset structure, data integrity, transformations on a subset of our data and verify the correctness of our pipeline.
* **[test.ipynb](test.ipynb)**: SQL queries to test the data piped into our database.


# The database schema design and ETL pipeline:

| Column        | Data Type     | Size  |Constraints|
| ------------- |:-------------:| -----:|:---------:|
| songplay_id   | SERIAL        | -     |primary key|
| start_time    | bigint        | -     |      -    |
| user_id   | SERIAL        | -     |           |
| song_id   | SERIAL        | -     |           |
| artist_id   | SERIAL        | -     |           |
| session_id   | SERIAL        | -     |           |
| location   | SERIAL        | -     |primary key|
| user_agent   | SERIAL        | -     |primary key|

songplay_id	int	-	primary key
start_time	bigint	-	
user_id	int	-	
level	text	-	
song_id	varchar	20	
artist_id	varchar	20	
session_id	int	-	
location	text	-	
user_agent	text	-	

In order to enable Sparkify to analyze their data, a Relational Database Schema was created, which can be filled with an ETL pipeline.

The so-called star scheme enables the company to view the user behaviour over several dimensions.
The fact table is used to store all user song activities that contain the category "NextSong". Using this table, the company can relate and analyze the dimensions users, songs, artists and time.

In order to fill the relational database, an ETL pipeline is used, which makes it possible to extract the necessary information from the log files of the user behaviour as well as the corresponding master data of the songs and convert it into the schema.

* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time.

# How to run the python scripts

To create the database tables run the following command:

To create tables:
```bash
python3 create_tables.py
```
Run this below command to fill in the tables via ETL:
```bash
python3 etl.py
```





# Dataset used



The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.





