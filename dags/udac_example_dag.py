from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1), #(2019, 1, 12),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

REDSHIFT_CONN_ID="redshift"
AWS_CREDENTIALS_ID="aws_credentials"
S3_BUCKET="udacity-dend"
LOG_DATA="log-data/{execution_date.year}/{execution_date.month}/{ds}-events.json" #???
LOG_JSON_PATH="s3://udacity-dend/log_json_path.json"
SONG_DATA="song_data"
REGION = "us-west-2"
SQL_QUERIES = SqlQueries()

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id =AWS_CREDENTIALS_ID,
    table="staging_events",
    s3_bucket=S3_BUCKET,
    s3_key= LOG_DATA,
    json = LOG_JSON_PATH,
    region = REGION,
    truncate = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    aws_credentials_id =AWS_CREDENTIALS_ID,
    table="staging_songs",
    s3_bucket=S3_BUCKET,
    s3_key=SONG_DATA,
#     json = "auto",
    region = REGION,
    truncate = True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    sql = SQL_QUERIES.songplay_table_insert,
    table = "songplays",
    truncate = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    sql = SQL_QUERIES.user_table_insert,
    table = "users",
    truncate = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    sql = SQL_QUERIES.song_table_insert,
    table = "songs",
    truncate = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    sql = SQL_QUERIES.artist_table_insert,
    table = "artists",
    truncate = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    sql = SQL_QUERIES.time_table_insert,
    table = "time",
    truncate = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = REDSHIFT_CONN_ID,
    tests = [
        {
            "sql" : "SELECT count(*) from artists WHERE artistid IS NULL",
            "expected_result" : 0
        },
        {
            "sql" : "SELECT count(*) from songplays WHERE playid IS NULL",
            "expected_result" : 0
        },
        {
            "sql" : "SELECT count(*) from songs WHERE songid IS NULL",
            "expected_result" : 0
        },
        {
            "sql" : "SELECT count(*) from users WHERE userid IS NULL",
            "expected_result" : 0
        },
    
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator