import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=100),
    "start_date": datetime(2020, 7, 10),
    "email_on_retry": False
}

dag = DAG("udac_example_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval="0 * * * *",
          catchup=False
        )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="devairflow",
    s3_key="log_data",
    file_type="JSON"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="devairflow",
    s3_key="song_data/A/A/A",
    file_type="JSON"
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    tables=["songplays", "users", "songs", "artists", "time"],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)



start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator