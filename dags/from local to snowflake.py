from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
import snowflake.connector
import psycopg2
import os
import csv
import pandas as pd


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
        df = pd.read_sql(querey, conn)
        df.to_csv(f'/opt/airflow/dags/stagging/{file_name}.csv', index=False)
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
    merschant_data_extraction = PythonOperator(
        task_id='exrtract_merchant_data',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': 'select distinct merchant_id, merchant, category, merch_long, merch_lat from transactions',
            'file_name': 'merchant_data'
     }
    )
    
    # # extracting customer data from postgres and saving it to csv
    customer_data_extraction = PythonOperator(
        task_id='exrtract_customer_data',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': 'select distinct customer_id, cc_num, first, last, gender, dob, street, city, state, zip, lat, long, city_pop from transactions',
            'file_name': 'customer_data'
     }
    )
        
        
    # extracting transaction data from postgres and saving it to csv    
    transaction_data_extraction = PythonOperator(
        task_id='extract_transaction_data',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': 'select trans_num, trans_date_trans_time, customer_id, merchant_id, amt, is_fraud, unix_time from transactions',
            'file_name': 'transaction_data'
     }
    )



    # Loading merschent data into airflow staging area and then into targeted table
    merschent_dim_loding = PythonOperator(
        task_id='load_merchant_data',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/stagging/merchant_data.csv',
            'CSV_FILE_NAME': 'merchant_data.csv',
            'table_name': 'merchant_Dim'
        }
    )
    
    # Loading customer data into airflow staging area and then into targeted table
    customer_dim_loding = PythonOperator(
        task_id='load_customer_data',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/stagging/customer_data.csv',
            'CSV_FILE_NAME': 'customer_data.csv',
            'table_name': 'customer_Dim'
        }
    )

    # Loading transaction data into airflow staging area and then into targeted table
    fact_loding = PythonOperator(
        task_id='load_fact_data',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_PATH': '/opt/airflow/dags/stagging/transaction_data.csv',
            'CSV_FILE_NAME': 'transaction_data.csv',
            'table_name': 'transactions_fact'
        }
    )


merschant_data_extraction>> customer_data_extraction>> transaction_data_extraction>> merschent_dim_loding>> customer_dim_loding>> fact_loding