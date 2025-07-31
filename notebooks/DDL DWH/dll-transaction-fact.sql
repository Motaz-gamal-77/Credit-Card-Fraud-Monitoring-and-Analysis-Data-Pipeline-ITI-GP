-- 4. Fact Table: transactions_fact
CREATE TABLE transactions_fact (
    trans_num VARCHAR PRIMARY KEY,
    trans_date_trans_time TIMESTAMP,
    customer_id VARCHAR,
    merchant_id VARCHAR,
    amt DOUBLE PRECISION,
    distance_km DOUBLE PRECISION,
    is_fraud    INT,
    unix_time BIGINT,

    -- Foreign Keys.FRAUD_DWH.PUBLIC.AIRFLOW_STAGEFRAUD_DWH.PUBLIC.AIRFLOW_STAGE
    FOREIGN KEY (trans_date_trans_time) REFERENCES date_Dim(trans_date_trans_time),
    FOREIGN KEY (customer_id) REFERENCES customer_Dim(customer_id),
    FOREIGN KEY (merchant_id) REFERENCES merchant_Dim(merchant_id)
);