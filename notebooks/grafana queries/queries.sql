-- Top Categories by Legitimate Transactions

SELECT
  category,
  COUNT(*) AS frauds
FROM
  transactions
WHERE
  $__timeFilter(event_time)
  AND is_fraud = 0
GROUP BY
  category
ORDER BY
  frauds DESC;

-- Distribution of Legitimate Transactions by Age
SELECT
  age,
  COUNT(*) AS frauds
FROM
  transactions
WHERE
  $__timeFilter(event_time)
  AND is_fraud = 0
GROUP BY
  age
ORDER BY
  age;

-- Total Fraudulent Transactions (Sample of 50 Records)
SELECT
  SUM(is_fraud)
FROM
  transactions
LIMIT
  50


-- Geographic Distribution of Legitimate Transactions

SELECT
  merch_lat AS latitude,
  merch_long AS longitude,
  COUNT(*) AS frauds
FROM
  transactions
WHERE
  $__timeFilter(event_time)
  AND is_fraud = 0
GROUP BY
  latitude, longitude;


-- Count of Transactions (Sample of 50 Records)
SELECT
  COUNT(is_fraud)
FROM
  transactions
LIMIT
  50

-- Fraud Rate Over Time (Per Minute)
SELECT
  date_trunc('minute', event_time) AS time,
  ROUND(100.0 * SUM(is_fraud)::decimal / COUNT(*), 2) AS fraud_rate
FROM
  transactions
WHERE
  $__timeFilter(event_time)
GROUP BY
  1
ORDER BY
  1;

-- Recent Legitimate Transactions Details
SELECT
  event_time,
  cc_num,
  merchant,
  category,
  amt,
  city,
  state
FROM
  transactions
WHERE
  $__timeFilter(event_time)
  AND is_fraud = 0
ORDER BY
  event_time DESC

-- Number of Fraudulent Transactions Per Minute
SELECT
  date_trunc('minute', event_time) AS time,
  SUM(is_fraud) AS frauds
FROM
  transactions
WHERE
  $__timeFilter(event_time)
GROUP BY
  1
ORDER BY
  1;


-- Fraudulent Transactions Every 20 Seconds
SELECT
  date_bin('20 seconds', event_time, TIMESTAMP '2000-01-01 00:00:00') AS time,
  SUM(is_fraud) AS frauds
FROM
  transactions
WHERE
  $__timeFilter(event_time)
GROUP BY
  1
ORDER BY
  1;