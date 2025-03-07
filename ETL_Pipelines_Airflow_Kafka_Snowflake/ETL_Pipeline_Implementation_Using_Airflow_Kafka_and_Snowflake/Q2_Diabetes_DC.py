# Import libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import pandas as pd

# Configuration
DATA_URL = "https://raw.githubusercontent.com/dhanala/Datasets/master/pima-indians-diabetes.data.csv"
DATA_PATH = "C:\\Users\\chiru\\Documents\\Data Aquisition\\Mini Project"
CSV = "pima-indians-diabetes.csv"
TRANSFORMED_CSV = "transformed_data.csv"

# Snowflake credentials and connection details
SNOWFLAKE_ACCOUNT = 'hwmtvar-ls62444'
SNOWFLAKE_USER = 'dhanala15'
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DB = "CHIRU_DIT_45604_AIW01_D2024SP"
SNOWFLAKE_SCHEMA = "MINI_PROJECT"
SNOWFLAKE_WAREHOUSE = '' 
SNOWFLAKE_ROLE = 'ACCOUNTADMIN'
SNOWFLAKE_TABLE = "DIABETES"

# Define Default arguments
default_args = {
    'owner': 'chiranjeevi',
    'start_date': days_ago(0),
    'email': ['dccan23@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
dag = DAG(
    dag_id='Mini_Project_Q2',
    default_args=default_args,
    description='Mini_Project_Q2',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

# Prepare task with password as an environment variable
prepare = BashOperator(
    task_id='prepare',
    dag=dag,
    bash_command=f'''
        echo "Preparing...";
        echo "Creating Table on Snowflake...";
        snowsql --noup -a {SNOWFLAKE_ACCOUNT} -u {SNOWFLAKE_USER} -w {SNOWFLAKE_WAREHOUSE} -r {SNOWFLAKE_ROLE} -d {SNOWFLAKE_DB} -s {SNOWFLAKE_SCHEMA} -q "CREATE OR REPLACE TABLE {SNOWFLAKE_TABLE} (
            Pregnancies NUMBER,
            Glucose NUMBER,
            BloodPressure NUMBER,
            SkinThickness NUMBER,
            Insulin NUMBER,
            BMI FLOAT,
            DiabetesPedigreeFunction FLOAT,
            Age NUMBER,
            Outcome NUMBER
        );"
    ''',
    env={'SNOWSQL_PWD': SNOWFLAKE_PASSWORD}  
)




# Extract
extract = BashOperator(
    task_id='extract',
    dag=dag,
    bash_command=f'''
        echo "Extracting...";
        echo "Downloading File...";
        curl --output {DATA_PATH}/{CSV} {DATA_URL};
    '''
)

# Transform
def transform_data():
    header_list = ['Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness', 
                   'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age', 'Outcome']
    data = pd.read_csv(f'{DATA_PATH}/{CSV}', header=None, names=header_list)
    transformed_data = data[data.Glucose > 120]
    transformed_data.to_csv(f'{DATA_PATH}/{TRANSFORMED_CSV}', index=False)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

# Define Load task with environment variable for password
load = BashOperator(
    task_id='load',
    dag=dag,
    bash_command=f'''
        echo "Loading to Snowflake...";
        snowsql -a {SNOWFLAKE_ACCOUNT} -u {SNOWFLAKE_USER} -w {SNOWFLAKE_WAREHOUSE} -r {SNOWFLAKE_ROLE} -d {SNOWFLAKE_DB} -s {SNOWFLAKE_SCHEMA} -q "PUT file://{DATA_PATH}/{TRANSFORMED_CSV} @%{SNOWFLAKE_TABLE}; COPY INTO {SNOWFLAKE_TABLE} FROM @%{SNOWFLAKE_TABLE}/{TRANSFORMED_CSV} FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);"
    ''',
    env={'SNOWSQL_PWD': SNOWFLAKE_PASSWORD}
)


# Clear
clear = BashOperator(
    task_id='clear',
    dag=dag,
    bash_command=f'''
        echo "Clearing...";
        rm -f {DATA_PATH}/{CSV} {DATA_PATH}/{TRANSFORMED_CSV};
    '''
)

# Define Pipeline
prepare >> extract >> transform >> load >> clear
