from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from datetime import datetime, timedelta

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_stage_and_tables():
    cursor = get_snowflake_cursor()
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS USER_DB_CRICKET.raw;")
        
        cursor.execute("""
            CREATE OR REPLACE STAGE USER_DB_CRICKET.raw.blob_stage
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = 'CSV', skip_header = 1, field_optionally_enclosed_by = '"');
        """)
        
        # Create tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS USER_DB_CRICKET.raw.user_session_channel (
                userId int not NULL,
                sessionId varchar(32) primary key,
                channel varchar(32) default 'direct'
            );
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS USER_DB_CRICKET.raw.session_timestamp (
                sessionId varchar(32) primary key,
                ts timestamp
            );
        """)
        print("Stage and tables created successfully")
    except Exception as e:
        print(f"Error creating stage and tables: {e}")
        raise

@task
def load_data():
    cursor = get_snowflake_cursor()
    try:
        # Load data into user_session_channel
        cursor.execute("""
            COPY INTO USER_DB_CRICKET.raw.user_session_channel
            FROM @USER_DB_CRICKET.raw.blob_stage/user_session_channel.csv
            FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)
        
        cursor.execute("""
            COPY INTO USER_DB_CRICKET.raw.session_timestamp
            FROM @USER_DB_CRICKET.raw.blob_stage/session_timestamp.csv
            FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"');
        """)
        print("Data loaded into tables successfully")
    except Exception as e:
        print(f"Error loading data: {e}")
        raise

with DAG(
    dag_id='SimpleSnowflakeETL',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    schedule_interval='30 2 * * *',
    tags=['ETL']
) as dag:
    create_stage_and_tables() >> load_data()