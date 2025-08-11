from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime
import snowflake.connector
import psycopg2
import csv
import pandas as pd
import smtplib
import os
from email.message import EmailMessage




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
	    user='WAHDAN',
        password='Wahdan_wahdan6666',
        account='UBBXFAJ-GDB29522',
        warehouse='COMPUTE_WH',
        database='FRAUD_DWH',
        schema='PUBLIC'
    )
    cur = conn.cursor()
    return cur , conn



# Function to send email notifications 
def send_email_notification(sender_email, password, reciver_email, subject, body):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = reciver_email
    msg.set_content(body)

    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.starttls()
        smtp.login(sender_email, password)
        smtp.send_message(msg)



# Function to extract data from Postgres and save it to a CSV file
def extract_data_from_postgres_and_save_it_to_csv(querey, file_name):
    conn, cur = connect_to_Postgres()
    try:
        df = pd.read_sql(querey, conn)
        df.to_csv(f'/opt/airflow/data/stagging/{file_name}.csv', index=False)
    finally:
        cur.close() 
        conn.close()

# Function to load CSV file into Snowflake staging area
def loaad_csv_to_staging_area(CSV_PATH):
    cursor , conn = connect_to_snowflake()
    try:
        # Upload local file to internal stage
        cursor.execute(f"PUT file://{CSV_PATH} @AIRFLOW_STAGE OVERWRITE = TRUE")
    finally:
        cursor.close()


# Function to load CSV data into Snowflake staging area and then into targeted table
def load_csv_to_snowflake(CSV_FILE_NAME,table_name):
    cursor , conn = connect_to_snowflake()
    try:
        # Load data from stage into table
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @AIRFLOW_STAGE/{CSV_FILE_NAME}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE'
        """)
    finally:
        cursor.close()

# Function to perform incremental load into Snowflake
def execute_query(query):
    cursor, conn = connect_to_snowflake()
    try:
        cursor.execute(query)
    finally:
        cursor.close()



# -------------------------------------------------------------------------- #
# Incremental Load Queries
# -------------------------------------------------------------------------- #

# source 
# indx,trans_date_trans_time,cc_num,merchant,category,amt,first,last,gender,street,city,state,zip,lat,long,city_pop,job,dob,trans_num,unix_time,merch_lat,merch_long,is_fraud,event_time,age,distance_km,merchant_id,customer_id

# merchant_Dim       
# merchant_id,merchant, category, merch_long, merch_lat

incremental_load_to_merchant =  """
                                    MERGE INTO merchant_Dim t
                                    USING merchant_Dim_s s
                                    ON t.merchant_id = s.merchant_id
                                    WHEN MATCHED THEN UPDATE SET
                                        t.merchant = s.merchant,
                                        t.category = s.category,
                                        t.merch_long = s.merch_long,
                                        t.merch_lat = s.merch_lat
                                    WHEN NOT MATCHED THEN
                                        INSERT (merchant_id,merchant, category, merch_long, merch_lat)
                                        VALUES (s.merchant_id, s.merchant, s.category, s.merch_long, s.merch_lat);
                                """




# customer_Dim
# customer_id, cc_num, first, last, gender, dob, age, job, street, city, state, zip, lat, long, city_pop

incremental_load_to_customer  =  """
                                    MERGE INTO customer_Dim t
                                    USING customer_Dim_s s
                                    ON t.customer_id = s.customer_id
                                    WHEN MATCHED THEN UPDATE SET
                                        t.cc_num = s.cc_num,
                                        t.first = s.first,
                                        t.last = s.last,
                                        t.gender = s.gender,
                                        t.dob = s.dob,
                                        t.age = s.age,
                                        t.job = s.job,
                                        t.street = s.street,
                                        t.city = s.city,
                                        t.state = s.state,
                                        t.zip = s.zip,
                                        t.lat = s.lat,
                                        t.long = s.long,
                                        t.city_pop = s.city_pop
                                    WHEN NOT MATCHED THEN
                                        INSERT (customer_id, cc_num, first, last, gender, dob, age, job, street, city, state, zip, lat, long, city_pop)
                                        VALUES (s.customer_id, s.cc_num, s.first, s.last, s.gender, s.dob, s.age, s.job, s.street, s.city, s.state, s.zip, s.lat, s.long, s.city_pop);
                                """

# transactions
# trans_num, trans_date_trans_time, customer_id, merchant_id, amt, distance_km, is_fraud, unix_time           
incremental_load_to_transaction =  """
                                    MERGE INTO transactions_fact t
                                    USING transactions_fact_s s
                                    ON t.trans_num = s.trans_num
                                    WHEN MATCHED THEN UPDATE SET
                                        t.trans_date_trans_time = s.trans_date_trans_time,
                                        t.customer_id = s.customer_id,
                                        t.merchant_id = s.merchant_id,
                                        t.amt = s.amt,
                                        t.distance_km = s.distance_km,
                                        t.is_fraud = s.is_fraud,
                                        t.unix_time = s.unix_time
                                    WHEN NOT MATCHED THEN
                                        INSERT (trans_num, trans_date_trans_time, customer_id, merchant_id, amt, distance_km, is_fraud, unix_time)
                                        VALUES (s.trans_num, s.trans_date_trans_time, s.customer_id, s.merchant_id, s.amt, s.distance_km, s.is_fraud, s.unix_time);
                                """



        
           

# --------------------------------------------------------------------------------------- #
        
# Main DAG definition
with DAG(
    dag_id='ETL_DAG_From_DB_To_DWH',
    start_date=datetime(2025, 7, 29),
    schedule_interval='10 0 * * *',
    catchup=False
) as dag:
    # --------------------------------------------------------------------------------------- #
    # extracting merschent data from postgres and saving it to csv
    merschant_data_extraction = PythonOperator(
        task_id='exrtract_merchant_data',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': "select distinct on (merchant_id) merchant_id,merchant, category, merch_long, merch_lat from transactions WHERE event_time::date = CURRENT_DATE - INTERVAL '1 day' ",
            'file_name': 'merchant_data'})
    
    # # extracting customer data from postgres and saving it to csv
    customer_data_extraction = PythonOperator(
        task_id='exrtract_customer_data',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': "select distinct on (customer_id) customer_id, cc_num, first, last, gender, dob, age, job, street, city, state, zip, lat, long, city_pop from transactions WHERE event_time::date = CURRENT_DATE - INTERVAL '1 day'",
            'file_name': 'customer_data'})
        
        
    # extracting transaction data from postgres and saving it to csv    
    transaction_data_extraction = PythonOperator(
        task_id='extract_transaction_data',
        python_callable=extract_data_from_postgres_and_save_it_to_csv,
        op_kwargs={
            'querey': "select distinct on (trans_num) trans_num, trans_date_trans_time, customer_id, merchant_id, amt, distance_km, is_fraud, unix_time from transactions WHERE event_time::date = CURRENT_DATE - INTERVAL '1 day'",
            'file_name': 'transaction_data'})

    # --------------------------------------------------------------------------------------- #
    # Loading merschent data into airflow staging area
    loding_merschent_csv_into_staging = PythonOperator(
        task_id='load_merchant_data_into_staging',
        python_callable=loaad_csv_to_staging_area,
        op_kwargs={'CSV_PATH': '/opt/airflow/data/stagging/merchant_data.csv'})

    # Loading customer data into airflow staging area
    loding_customer_csv_into_staging = PythonOperator(
        task_id='load_customer_data_into_staging',
        python_callable=loaad_csv_to_staging_area,
        op_kwargs={'CSV_PATH': '/opt/airflow/data/stagging/customer_data.csv'})

    # Loading transaction data into airflow staging area
    loding_transaction_csv_into_staging = PythonOperator(
        task_id='load_transaction_data_into_staging',
        python_callable=loaad_csv_to_staging_area,
        op_kwargs={'CSV_PATH': '/opt/airflow/data/stagging/transaction_data.csv'})

    # --------------------------------------------------------------------------------------- #
    # Loading merschent data from airflow staging area and then into targeted staging table
    merschent_staging_table_loading = PythonOperator(
        task_id='load_merchant_data',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_FILE_NAME': 'merchant_data.csv',
            'table_name': 'merchant_Dim_s'
        }
    )

    # Loading customer data from airflow staging area and then into targeted staging table
    customer_staging_table_loding = PythonOperator(
        task_id='load_customer_data',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_FILE_NAME': 'customer_data.csv',
            'table_name': 'customer_Dim_s'
        }
    )

    # Loading transaction data from airflow staging area and then into targeted staging table
    fact_staging_table_loding = PythonOperator(
        task_id='load_fact_data',
        python_callable=load_csv_to_snowflake,
        op_kwargs={
            'CSV_FILE_NAME': 'transaction_data.csv',
            'table_name': 'transactions_fact_s'
        }
    )
    # --------------------------------------------------------------------------------------- #
    # incremental load to merchant_Dim
    incremental_load_to_merchant_Dim = PythonOperator(
        task_id='incremental_load_to_merchant_Dim',
        python_callable=execute_query,
        op_kwargs={
            'query': incremental_load_to_merchant
        }
    )

    # incremental load to customer_Dim
    incremental_load_to_customer_Dim = PythonOperator(
        task_id='incremental_load_to_customer_Dim',
        python_callable=execute_query,
        op_kwargs={
            'query': incremental_load_to_customer
        }
    )

    # incremental load to transaction_fact
    incremental_load_to_transaction_fact = PythonOperator(
        task_id='incremental_load_to_transaction_fact',
        python_callable=execute_query,
        op_kwargs={
            'query': incremental_load_to_transaction
        }
    )
    # --------------------------------------------------------------------------------------- #
    # truncate staging tables
    truncate_merchant_Dim_s = PythonOperator(
        task_id='truncate_merchant_Dim_s',
        python_callable=execute_query,
        op_kwargs={
            'query': "TRUNCATE TABLE merchant_Dim_s;"
        }
    )
    
    # truncate staging tables
    truncate_customer_Dim_s = PythonOperator(
        task_id='truncate_customer_Dim_s',
        python_callable=execute_query,
        op_kwargs={
            'query': "TRUNCATE TABLE customer_Dim_s;"
        }
    )
    
    # truncate staging tables
    truncate_fact_s = PythonOperator(
        task_id='truncate_fact_s',
        python_callable=execute_query,
        op_kwargs={
            'query': "TRUNCATE TABLE transactions_fact_s;"
        }
    )

    # send email notification
    # send_email_notification = PythonOperator(
    #     task_id='send_email_notification',
    #     python_callable=send_email_notification,
    #     op_kwargs={
    #         'sender_email': "",
    #         'password': "",
    #         'reciver_email': "",
    #         'subject': 'ETL Job Notification',
    #         'body': 'The ETL job has completed successfully.'
    #     }
    # )
# [
#     [merschant_data_extraction >> loding_merschent_csv_into_staging >> merschent_staging_table_loading >> incremental_load_to_merchant_Dim >> truncate_merchant_Dim_s] ,
#     [customer_data_extraction >> loding_customer_csv_into_staging >> customer_staging_table_loding >> incremental_load_to_customer_Dim >> truncate_customer_Dim_s] 
# ] >> [transaction_data_extraction >> loding_transaction_csv_into_staging >> fact_staging_table_loding >> incremental_load_to_transaction_fact >> truncate_fact_s]


merchant_chain = (
    merschant_data_extraction
    >> loding_merschent_csv_into_staging
    >> merschent_staging_table_loading
    >> incremental_load_to_merchant_Dim
    
)

customer_chain = (
    customer_data_extraction
    >> loding_customer_csv_into_staging
    >> customer_staging_table_loding
    >> incremental_load_to_customer_Dim
    
)

transaction_chain = (
    transaction_data_extraction
    >> loding_transaction_csv_into_staging
    >> fact_staging_table_loding
    >> incremental_load_to_transaction_fact
    
)

[merchant_chain, customer_chain] >> transaction_chain >> [truncate_fact_s  , truncate_customer_Dim_s  , truncate_merchant_Dim_s]