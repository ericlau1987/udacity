from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from custom_operators.stage_redshift import StageToRedshiftOperator
from custom_operators.load_fact import LoadFactOperator
from custom_operators.load_dimension import LoadDimensionOperator
from custom_operators.data_quality import DataQualityOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_stage_schema = PostgresOperator(
        task_id="create_staging_schema",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_schema.format('staging')
    )
    # create staging event
    drop_stage_events_table = PostgresOperator(
        task_id="drop_staging_events_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.drop_table.format('staging', 'staging_events')
    )

    create_events_table = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.staging_events_table_create
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        schema='staging',
        table='staging_events',
        s3_bucket='ericliu-udacity-lake-house',
        s3_key='log-data',
        delimiter=",",
        ignore_headers=1,
        sql = SqlQueries.staging_events_copy
    )

    # create staging songs
    drop_stage_songs_table = PostgresOperator(
        task_id="drop_staging_songs_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.drop_table.format('staging', 'staging_songs')
    )

    create_songs_table = PostgresOperator(
        task_id="create_staging_songs_table",
        postgres_conn_id="redshift",
        sql=SqlQueries.staging_songs_table_create
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        schema='staging',
        table='staging_songs',
        s3_bucket='ericliu-udacity-lake-house',
        s3_key='song-data',
        delimiter=",",
        ignore_headers=1,
        sql = SqlQueries.staging_songs_copy
    )

    create_fact_schema = PostgresOperator(
        task_id="create_fact_schema",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_schema.format('fact')
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        schema='fact',
        table='fact_song_plays',
        sql = SqlQueries.songplay_table_insert
    )

    create_dim_schema = PostgresOperator(
        task_id="create_dim_schema",
        postgres_conn_id="redshift",
        sql=SqlQueries.create_schema.format('dim')
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        schema='dim',
        table='dim_users',
        sql = SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        schema='dim',
        table='dim_songs',
        sql = SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        schema='dim',
        table='dim_artists',
        sql = SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        schema='dim',
        table='dim_time',
        sql = SqlQueries.time_table_insert
    )

    # run_quality_checks = DataQualityOperator(
    #     task_id='Run_data_quality_checks',
    # )

    start_operator >> create_stage_schema >> drop_stage_events_table >> create_events_table >> stage_events_to_redshift >> create_fact_schema >> load_songplays_table
    start_operator >> create_stage_schema >> drop_stage_songs_table >> create_songs_table >> stage_songs_to_redshift >> create_fact_schema >> load_songplays_table
    load_songplays_table >> create_dim_schema >> load_user_dimension_table >> load_song_dimension_table >> load_artist_dimension_table >> load_time_dimension_table


final_project_dag = final_project()