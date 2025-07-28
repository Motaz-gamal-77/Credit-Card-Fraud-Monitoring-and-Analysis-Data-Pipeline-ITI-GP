from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
import snowflake.connector
import psycopg2
import os
import csv


# connect to Postgres
def connect_to_Postgres():
    print("Connecting to Billing_DWH..... ")
    conn = psycopg2.connect(
        user='spark',
        password='spark',
        host='postgres_v2',
        port='5432',         
        database='sparkdb'
    )
    cur = conn.cursor()
    return conn ,cur


# connect to Snowflake
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


# Function to extract data from Postgres and save it to a CSV file
def extract_data_from_postgres_and_save_it_to_csv(querey, file_name):
    conn, cur = connect_to_Postgres()
    try:
        # Example query to extract data
        cur.execute(f"{querey}")
        rows = cur.fetchall()

        # Save to CSV file
        csv_file_path = f'/opt/airflow/dags/data/{file_name}.csv'
        with open(csv_file_path, 'w') as f:
            for row in rows:
                f.write(','.join(map(str, row)) + '\n')
    finally:
        cur.close() 
        conn.close()



# Function to load CSV data into Snowflake staging area and then into targeted table
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
        

        
# Main DAG definition
with DAG(
    dag_id='list_snowflake_tables',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # extracting merschent data from postgres and saving it to csv
    merschent_data_extraction = PythonOperator(
        task_id='get_table_names',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': 'query to extract data from postgres',
            'file_name': 'file_name_you_want_to_save_data_in.csv'
     }
    )
    
    # extracting customer data from postgres and saving it to csv
    customer_data_extraction = PythonOperator(
        task_id='get_table_names',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': 'query to extract data from postgres',
            'file_name': 'file_name_you_want_to_save_data_in.csv'
     }
    )
        
        
    # extracting transaction data from postgres and saving it to csv    
    transaction_data_extraction = PythonOperator(
        task_id='get_table_names',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': 'query to extract data from postgres',
            'file_name': 'file_name_you_want_to_save_data_in.csv'
     }
    )



    # Loading merschent data into airflow staging area and then into targeted table
    merschent_dim_loding = PythonOperator(
        task_id='get_table_names',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/data/fact.csv',
            'CSV_FILE_NAME': 'file_name_in_staging_area',
            'table_name': 'table_name_in_dwh'
        }
    )
    
    # Loading customer data into airflow staging area and then into targeted table
    customer_dim_loding = PythonOperator(
        task_id='get_table_names',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/data/fact.csv',
            'CSV_FILE_NAME': 'file_name_in_staging_area',
            'table_name': 'table_name_in_dwh'
        }
    )

    # Loading transaction data into airflow staging area and then into targeted table
    fact_loding = PythonOperator(
        task_id='get_table_names',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/data/fact.csv',
            'CSV_FILE_NAME': 'file_name_in_staging_area',
            'table_name': 'table_name_in_dwh'
        }
    )