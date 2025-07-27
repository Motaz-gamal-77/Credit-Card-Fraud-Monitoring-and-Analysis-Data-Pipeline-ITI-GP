from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import os


def connect_to_snowflake():
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user='MOATAZGAMAL710',
        password='@123456789123Dwh',  # Ideally use os.environ.get('snowflake_pass')
        account='YREDIGW-DB28773',
        warehouse='COMPUTE_WH',
        database='FRAUD_DWH',
        schema='PUBLIC'
    )
    cur = conn.cursor()
    return cur , conn



def load_csv_to_snowflake(CSV_PATH, CSV_FILE_NAME,table_name):
    cursor , conn = connect_to_snowflake()
    try:

        # Upload local file to internal stage
        cursor.execute(f"PUT file://{CSV_PATH} @AIRFLOW_STAGE OVERWRITE = TRUE")

        # Load data from stage into table
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @AIRFLOW_STAGE/{CSV_FILE_NAME}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """)
    finally:
        cursor.close()
        
        

with DAG(
    dag_id='list_snowflake_tables',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    fact_loding = PythonOperator(
        task_id='get_table_names',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/data/fact.csv',
            'CSV_FILE_NAME': 'file_name_in_staging_area',
            'table_name': 'table_name_in_dwh'
        }
        
    )
    
    merschent_dim_loding = PythonOperator(
        task_id='get_table_names',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/data/fact.csv',
            'CSV_FILE_NAME': 'file_name_in_staging_area',
            'table_name': 'table_name_in_dwh'
        }
        
    )
    
    
    customer_dim_loding = PythonOperator(
        task_id='get_table_names',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/data/fact.csv',
            'CSV_FILE_NAME': 'file_name_in_staging_area',
            'table_name': 'table_name_in_dwh'
        }
        
    )

