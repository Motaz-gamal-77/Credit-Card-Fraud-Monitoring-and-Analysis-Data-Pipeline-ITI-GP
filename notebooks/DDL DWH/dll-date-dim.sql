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
