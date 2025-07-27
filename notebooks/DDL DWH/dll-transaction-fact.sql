-- 4. Fact Table: transactions_fact
CREATE TABLE transactions_fact (
    trans_num VARCHAR PRIMARY KEY,
    time_id INT,
    customer_id VARCHAR,
    merchant_id VARCHAR,
    amt DOUBLE PRECISION,
    distance_km DOUBLE PRECISION,
    is_fraud    INT,
    unix_time BIGINT,

    -- Foreign Keys.
    FOREIGN KEY (time_id) REFERENCES date_Dim(time_id),
    FOREIGN KEY (customer_id) REFERENCES customer_Dim(customer_id),
    FOREIGN KEY (merchant_id) REFERENCES merchant_Dim(merchant_id)
);
