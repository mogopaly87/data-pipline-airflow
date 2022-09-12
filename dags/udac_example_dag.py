from datetime import datetime, timedelta
import airflow.utils.dates as dates
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)
# from plugins.operators import StageToRedshiftOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': dates.days_ago(1),
}

with DAG(
    dag_id = 'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@daily',
    start_date=dates.days_ago(1)
) as dag:
    

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend2-mogo",
        s3_key="log_data",
        table="staging_events",
        jsonpaths='s3://udacity-dend2-mogo/jsonpaths/jpath.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="udacity-dend2-mogo",
        s3_key="song_data",
        table="staging_songs",
        jsonpaths='s3://udacity-dend2-mogo/jsonpaths/jsonpath_songs.json'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        target_table=["users", "songs", "artists", "time", "songplays"]
    )

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table 

load_songplays_table >> [load_user_dimension_table,
                        load_song_dimension_table,
                        load_artist_dimension_table,
                        load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator