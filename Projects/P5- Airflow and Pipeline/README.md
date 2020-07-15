# Data Pipelines
## Introduction:
Goal: create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## This documentation contains:

1. Table design and schemas
2. Data extraction and transformation
3. Explanation of files in the repository
4. Running the scripts

### 1. Table design and schemas
#### The required DAG

<img src="./images/airflow_dag.JPG?raw=true" width="800" />

### Tables:
- Fact Table: songplays
- Dimension Tables: users, songs, artists and time.
- Staging Tables: Stage_events, Stage_songs


### 2. Data extraction and transformation.

In order to enable Sparkify to analyze their data, a Relational Database Schema was created, which can be filled with an ETL pipeline.

#### Dataset used

The data is queried from s3 buckets hosted at AWS

* **Song data**: ```s3://udacity-dend/song_data```
* **Log data**: ```s3://udacity-dend/log_data```

### 3. Explanation of files in the repository

* **[airflow](airflow)**: workspace folder storing the airflow DAGs and plugins used by the airflow server.
* /
    * `create_tables.sql` - Contains the DDL for all tables used in this projecs
* dags
    * `udac_example_dag.py` - The DAG configuration file to run in Airflow
* **[airflow/dags/s3_to_redshift_dag.py](airflow/dags/s3_to_redshift_dag.py)**: "Main"-DAG written in Python containing all steps of the pipeline.
    * helpers
        * `sql_queries` - Redshift statements used in the DAG
* **[airflow/plugins/operators/](airflow/plugins/operators)**: Folder containing such called operators which can be called inside an Airflow DAG. These are python classes to outsource and build generic functions as modules of the DAG.
* plugins
    * operators
        * `stage_redshift.py` - Operator to read files from S3 and load into Redshift staging tables
        * `load_fact.py` - Operator to load the fact table in Redshift
        * `load_dimension.py` - Operator to read from staging tables and load the dimension tables in Redshift
        * `data_quality.py` - Operator for data quality checking


### 4. Running Scripts
- In udacity:  Start an Airflow server in the Udacity workspace. Run **/opt/airflow/start.sh**  command to start the Airflow webserver.
