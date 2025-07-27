-- 3. Dimension: date_Dim
CREATE TABLE date_Dim (
    time_id INT PRIMARY KEY,
    trans_date_trans_time TIMESTAMP,
    day INT,
    month INT,
    year INT,
    hour INT,
    minute INT
);

