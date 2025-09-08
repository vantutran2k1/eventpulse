CREATE NAMESPACE IF NOT EXISTS eventpulse.bronze;

CREATE TABLE IF NOT EXISTS eventpulse.bronze.clicks_raw (
    user_id STRING,
    session_id STRING,
    event_type STRING,
    url STRING,
    referer STRING,
    user_agent STRING,
    ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ts))
LOCATION 's3://warehouse/bronze/clicks_raw';

CREATE TABLE IF NOT EXISTS eventpulse.bronze.orders_cdc (
    order_id STRING,
    user_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    status STRING,
    ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ts))
LOCATION 's3://warehouse/bronze/orders_cdc';

CREATE TABLE IF NOT EXISTS eventpulse.bronze.payments_cdc (
    payment_id STRING,
    order_id STRING,
    user_id STRING,
    amount DOUBLE,
    status STRING,
    ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ts))
LOCATION 's3://warehouse/bronze/payments_cdc';
