import datetime



from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageS3ToRedshiftOperator,LoadFactOperator,LoadDimensionOperator,DataQualityOperator,CreateRedshiftTables)
from airflow.hooks
# Create Tables
# Staging Tables into AWS Redshift
# Processing Data
# Loading Sonplays Fact Table
# Loading Dimension Tables
# Quality Checks on Data
# End




default_args = {
    'owner':'hamza',
    'start_date':datetime.datetime.now(),
    'depends_on_past':False,
    'retries':2,
    'retry_delay':datetime.timedelta(minutes=10),
    'email_on_retry':False
}


dag = DAG('project_dag',default_args=default_args,schedule_interval="@hourly")


start_operator = DummyOperator(task_id="start_execution",dag=dag)


# Creating tables


create_tables = CreateRedshiftTables(task_id="create_tables",redshift_conn_id="redshift",file_path="helpers/create_table_script.sql")
# Stage Events :
stage_events = StageS3ToRedshiftOperator(task_id = "events_stage",
                                         aws_connection='aws_conn',
                                         redshift_conn_id= 'redshift',
                                         table='events',
                                         provide_context=True,
                                         dag=dag,
                                         s3_bucket="myproject",
                                         s3_key="events_data",
                                         region="us-west-2",
                                         format_data="JSON")

# Stage Songs
stage_songs = StageS3ToRedshiftOperator(task_id = "songs_stage",
                                        aws_connection='aws_conn',
                                        redshift_conn_id= 'redshift',
                                        table='songs',
                                        provide_context=True,
                                         dag=dag,
                                        s3_bucket="myproject",
                                        s3_key="songs_data",
                                        region="us-west-2",
                                        format_data="JSON")


# Loading Data into Fact Table
load_songsplays_fact_table = LoadFactOperator(task_id="load_songsplays_fact_table",
                                              dag=dag,
                                              redshift_connection_id="redshift",
                                              sql_query="""
                                              SELECT
                                                    md5(events.sessionid || events.start_time) songplay_id,
                                                    events.start_time, 
                                                    events.userid, 
                                                    events.level, 
                                                    songs.song_id, 
                                                    songs.artist_id, 
                                                    events.sessionid, 
                                                    events.location, 
                                                    events.useragent
                                                    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                                                FROM staging_events
                                                WHERE page='NextSong') events
                                                LEFT JOIN staging_songs songs
                                                ON events.song = songs.title
                                                    AND events.artist = songs.artist_name
                                                    AND events.length = songs.duration
                                              """)

# Loading Dimensions Tables

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql_query = """
    SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """,
    truncate= True
)


load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql_query="""
    SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
   """,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql_query="""
   SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
   """,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql_query="""
   SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
   """,
    truncate=True
)

# Checking Data quality

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = ["users","songs","artists","time","songsplays"]
)

end_operator = DummyOperator(task_id="stop_execution",dag=dag)

# Order of Eexcution

create_tables >> [stage_events , stage_songs]
[stage_events, stage_songs] >> load_songsplays_fact_table
load_songsplays_fact_table >> [load_song_dimension_table , load_time_dimension_table  , load_user_dimension_table , load_artist_dimension_table]
[load_artist_dimension_table , load_song_dimension_table  , load_time_dimension_table , load_user_dimension_table] >> run_quality_checks

