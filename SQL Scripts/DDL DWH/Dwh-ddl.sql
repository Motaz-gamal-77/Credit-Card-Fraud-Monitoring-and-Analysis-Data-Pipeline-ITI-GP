-- 1. Dimension: customer_Dim
CREATE TABLE customer_Dim (
    customer_id VARCHAR PRIMARY KEY,
    cc_num VARCHAR,
    first VARCHAR,
    last VARCHAR,
    gender VARCHAR,
    dob DATE,
    age INT not null,
    job VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip INT,
    lat DOUBLE PRECISION,
    long DOUBLE PRECISION,
    city_pop INT
);

-- 2. Dimension: merchant_Dim
CREATE TABLE merchant_Dim (
    merchant_id VARCHAR PRIMARY KEY,
    merchant VARCHAR,
    category VARCHAR,
    merch_long DOUBLE PRECISION,
    mech_lat DOUBLE PRECISION   
);

-- 3. Dimension: date_Dim
CREATE TABLE date_Dim (
    trans_date_trans_time TIMESTAMP PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    hour INT,
    minute INT
);



-- Set start and end date
SET start_date = TO_TIMESTAMP('2020-01-01 00:00:00');
SET end_date = CURRENT_TIMESTAMP();

-- Create and populate date_Dim
INSERT INTO date_Dim
SELECT
    date_value AS trans_date_trans_time,
    EXTRACT(DAY FROM date_value) AS day,
    EXTRACT(MONTH FROM date_value) AS month,
    EXTRACT(YEAR FROM date_value) AS year,
    EXTRACT(HOUR FROM date_value) AS hour,
    EXTRACT(MINUTE FROM date_value) AS minute
FROM (
    SELECT
        DATEADD(MINUTE, SEQ4(), $start_date) AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 10000000))  -- You may need more than one batch
    WHERE DATEADD(MINUTE, SEQ4(), $start_date) <= $end_date
);

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

--truncate table customer_dim;
--truncate table merchant_dim;
--truncate table transactions_fact;