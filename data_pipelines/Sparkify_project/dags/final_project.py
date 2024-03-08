from datetime import datetime, timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.create_tables import CreateTablesOperator
from final_project_operators.sql_queries import SqlQueries
from udacity.common import final_project_sql_statements


default_args = {
    'owner': 'srudston',
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'email_on_retry': False
}

dag_name='sparkify_dag'

dag = DAG(dag_name, default_args=default_args, schedule_interval='0 * * * *', max_active_runs=3, start_date=datetime(2018, 11, 1))

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
    
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="sarah-rudston",
    s3_key="log-data/{execution_date.year}/{execution_date.month}/",
    data_format="json",
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="sarah-rudston",
        s3_key="song_data",
        ignore_headers="0",
        data_format="json",
        provide_context=True,
    )

load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        provide_context=True,
        sql_query=SqlQueries.songplays_table_insert,
    )

load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dim_table',
        redshift_conn_id = "redshift",
        destination_table = "users",
        sql_query=SqlQueries.user_table_insert,
        update_mode="insert",
    )

load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dim_table',
        redshift_conn_id="redshift",
        destination_table="songs",
        sql_query=SqlQueries.song_table_insert,
        update_mode="insert",
    )

load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        redshift_conn_id="redshift",
        destination_table="artists",
        sql_query=SqlQueries.artist_table_insert,
        update_mode="insert",
    )

load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dim_table',
        redshift_conn_id="redshift",
        destination_table="time",
        sql_query=SqlQueries.time_table_insert,
        update_mode="insert",
    )

run_quality_checks = DataQualityOperator(
        task_id='run_data_quality_checks',
        provide_context=True,
        aws_credentials_id="aws_credentials",
        redshift_conn_id="redshift",
        tables=["songplay", "users", "song", "artist", "time"]
    )

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks 
run_quality_checks >> end_operator