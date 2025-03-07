#!/usr/bin/env python3

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import snowflake.connector
import os

# It pulls the current working directory
current_directory = os.getcwd()

# It defines default args
default_args = {
    'owner': 'chiranjeevi',
    'start_date': datetime(2024, 5, 28)
}

# It defines DAG
dag = DAG(
    'web_server_log_acces_etl',
    default_args=default_args,
    schedule='@daily'
)

# Here it defines functions for tasks
def fetch_data():
    url = 'https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/web-server-access-log.txt.gz'
    df = pd.read_csv(url, sep="#", compression='gzip')
    first_four_columns = df.iloc[:, :4]
    first_four_columns.to_csv('/tmp/server_logs.csv', index=False, header=False)
    print('Data fetched...')

def process_data():
    input_file = '/tmp/server_logs.csv'
    output_file = '/tmp/processed_server_logs.csv'

    df = pd.read_csv(input_file)
    df.to_csv(output_file, index=False, header=False)
    print('Data processed...')

def upload_data():
    print('Uploading data...')
    conn = snowflake.connector.connect(
        user=os.getenv('snowflake_user'),
        password=os.getenv('snowflake_password'),
        account=os.getenv('snowflake_account'),
        database=os.getenv('snowflake_database'),
        schema=os.getenv('snowflake_schema'),
        warehouse=os.getenv('snowflake_warehouse')
    )
    cursor = conn.cursor()

    cursor.execute('USE DATABASE CHIRU_DIT_45604_AIW01_D2024SP')
    cursor.execute('USE SCHEMA MINI_PROJECT')
    cursor.execute('''
        CREATE OR REPLACE TABLE Web_Access_Logs (
            log_timestamp TIMESTAMP,
            lat FLOAT,
            long FLOAT,
            visitor_id VARCHAR(42)
        )
    ''')
    cursor.execute('''
        CREATE OR REPLACE FILE FORMAT MY_CUSTOM_CSV
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 0
    ''')
    cursor.execute('''
        CREATE OR REPLACE STAGE MY_CUSTOM_STAGE
        FILE_FORMAT = MY_CUSTOM_CSV
    ''')

    put_command = f"PUT file:///tmp/processed_server_logs.csv @MY_CUSTOM_STAGE"
    print(put_command)
    cursor.execute(put_command)

    cursor.execute('''
        COPY INTO Web_Access_Logs
        FROM @MY_CUSTOM_STAGE/processed_server_logs.csv
        FILE_FORMAT = (FORMAT_NAME = MY_CUSTOM_CSV)
    ''')

    cursor.close()
    conn.close()
    print('Data upload complete.')

# It defines task
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_data',
    python_callable=upload_data,
    dag=dag
)

# Set task dependencies
fetch_task >> process_task >> upload_task
