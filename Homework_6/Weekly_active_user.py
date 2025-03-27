from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Constants
SNOWFLAKE_CONN_ID = "snowflake_conn"
RAW_SCHEMA = "USER_DB_CRICKET.raw"
ANALYTICS_SCHEMA = "USER_DB_CRICKET.analytics"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="wau_etl_pipeline",
    default_args=default_args,
    schedule_interval="@weekly",  # Run every week
    catchup=False,
    tags=["snowflake", "wau"],
) as dag:

    # Task 1: Create the target table if it doesn't exist
    create_table = SnowflakeOperator(
        task_id="create_weekly_active_users_table",
        sql=f"""
        CREATE TABLE IF NOT EXISTS {ANALYTICS_SCHEMA}.weekly_active_users (
            week_start DATE PRIMARY KEY,
            active_users INT
        );
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task 2: Compute weekly active users and load into the target table
    compute_wau = SnowflakeOperator(
        task_id="compute_weekly_active_users",
        sql=f"""
        INSERT INTO {ANALYTICS_SCHEMA}.weekly_active_users
        SELECT
            DATE_TRUNC('WEEK', ts) AS week_start,
            COUNT(DISTINCT userid) AS active_users
        FROM
            {RAW_SCHEMA}.user_session_channel usc
        JOIN
            {RAW_SCHEMA}.session_timestamp st
        ON
            usc.sessionid = st.sessionid
        GROUP BY 1
        ORDER BY 1;
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task dependencies
    create_table >> compute_wau