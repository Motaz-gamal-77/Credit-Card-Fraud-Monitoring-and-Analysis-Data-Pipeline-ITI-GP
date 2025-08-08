-- 2. Dimension: merchant_Dim
CREATE TABLE merchant_Dim (
    merchant_id VARCHAR PRIMARY KEY,
    merchant VARCHAR,
    category VARCHAR,
    merch_long DOUBLE PRECISION,
    mech_lat DOUBLE PRECISION   
);
