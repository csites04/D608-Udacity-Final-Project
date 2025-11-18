from datetime import timedelta
import pendulum
import logging

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from helpers.sql_queries import SqlQueries

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

default_args = {
    'owner': 'caitlin',
    'start_date': pendulum.datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule='0 * * * *',
    tags=['sparkify', 'etl', 'redshift']
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='Stop_execution')

    create_artists_table = PostgresOperator(
        task_id="create_artists_table",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS public.artists (
                artistid varchar(256) NOT NULL,
                name varchar(512),
                location varchar(512),
                lattitude numeric(18,0),
                longitude numeric(18,0)
            );
        """
    )

    create_songplays_table = PostgresOperator(
        task_id="create_songplays_table",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS public.songplays (
                playid varchar(32) NOT NULL,
                start_time timestamp NOT NULL,
                userid int4 NOT NULL,
                "level" varchar(256),
                songid varchar(256),
                artistid varchar(256),
                sessionid int4,
                location varchar(256),
                user_agent varchar(256),
                CONSTRAINT songplays_pkey PRIMARY KEY (playid)
            );
        """
    )

    create_songs_table = PostgresOperator(
        task_id="create_songs_table",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS public.songs (
                songid varchar(256) NOT NULL,
                title varchar(512),
                artistid varchar(256),
                "year" int4,
                duration numeric(18,0),
                CONSTRAINT songs_pkey PRIMARY KEY (songid)
            );
        """
    )

    create_staging_events_table = PostgresOperator(
        task_id="create_staging_events_table",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS public.staging_events (
                artist varchar(256),
                auth varchar(256),
                firstname varchar(256),
                gender varchar(256),
                iteminsession int4,
                lastname varchar(256),
                length numeric(18,0),
                "level" varchar(256),
                location varchar(256),
                "method" varchar(256),
                page varchar(256),
                registration numeric(18,0),
                sessionid int4,
                song varchar(256),
                status int4,
                ts int8,
                useragent varchar(256),
                userid int4
            );
        """
    )

    create_staging_songs_table = PostgresOperator(
        task_id="create_staging_songs_table",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS public.staging_songs (
                num_songs int4,
                artist_id varchar(256),
                artist_name varchar(512),
                artist_latitude numeric(18,0),
                artist_longitude numeric(18,0),
                artist_location varchar(512),
                song_id varchar(256),
                title varchar(512),
                duration numeric(18,0),
                "year" int4
            );
        """
    )

    create_time_table = PostgresOperator(
        task_id="create_time_table",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS public."time" (
                start_time timestamp NOT NULL,
                "hour" int4,
                "day" int4,
                week int4,
                "month" varchar(256),
                "year" int4,
                weekday varchar(256),
                CONSTRAINT time_pkey PRIMARY KEY (start_time)
            );
        """
    )

    create_users_table = PostgresOperator(
        task_id="create_users_table",
        postgres_conn_id="redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS public.users (
                userid int4 NOT NULL,
                first_name varchar(256),
                last_name varchar(256),
                gender varchar(256),
                "level" varchar(256),
                CONSTRAINT users_pkey PRIMARY KEY (userid)
            );
        """
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials', 
        s3_bucket='csites-project',
        s3_key='log-data',
        json_path='s3://csites-project/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials', 
        s3_bucket='csites-project',
        s3_key='song-data',
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        append_only=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        dq_checks=[
            {"check_sql": "SELECT COUNT(*) FROM users", "expected_result": 104},
            {"check_sql": "SELECT COUNT(*) FROM songs", "expected_result": 384637}
        ]
    )

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
