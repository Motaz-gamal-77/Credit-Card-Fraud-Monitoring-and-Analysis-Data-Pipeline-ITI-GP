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