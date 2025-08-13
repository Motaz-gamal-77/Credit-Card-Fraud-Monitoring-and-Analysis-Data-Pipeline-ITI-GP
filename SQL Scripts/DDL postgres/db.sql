CREATE Table Transactions (
    indx VARCHAR PRIMARY KEY ,
    trans_date_trans_time TIMESTAMP ,
    cc_num VARCHAR ,
    merchant VARCHAR ,
    category VARCHAR ,
    amt DOUBLE PRECISION NOT NULL,
    first VARCHAR ,
    last VARCHAR ,
    gender VARCHAR ,
    street VARCHAR ,
    city VARCHAR ,
    state VARCHAR ,
    zip INT ,
    lat DOUBLE PRECISION ,
    long DOUBLE PRECISION ,
    city_pop INT ,
    job VARCHAR ,
    dob DATE ,
    trans_num VARCHAR ,
    unix_time bigint ,
    merch_lat DOUBLE PRECISION ,
    merch_long DOUBLE PRECISION ,
    is_fraud INT ,
    event_time TIMESTAMP ,
    age INT not null ,
    distance_km DOUBLE PRECISION ,
    merchant_id VARCHAR ,
    customer_id VARCHAR
)

ALTER TABLE Transactions ADD COLUMN prediction INT DEFAULT 0;

DROP TABLE Transactions;

TRUNCATE TABLE Transactions;