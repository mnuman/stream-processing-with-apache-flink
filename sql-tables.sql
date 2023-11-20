SET sql-client.execution.result-mode = tableau;

CREATE TABLE transactions (
    transactionId      STRING,
    accountId          STRING,
    customerId         STRING,
    eventTime          BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(eventTime, 3),
    eventTimeFormatted STRING,
    type               STRING,
    operation          STRING,
    amount             DOUBLE,
    balance            DOUBLE,
        WATERMARK FOR eventTime_ltz AS eventTime_ltz
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'redpanda:9092', --//'kafka:29092' <-- for kafka use this
    'properties.group.id' = 'group.transactions',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);
CREATE TABLE customers (
    customerId STRING,
    sex STRING,
    social STRING,
    fullName STRING,
    phone STRING,
    email STRING,
    address1 STRING,
    address2 STRING,
    city STRING,
    state STRING,
    zipcode STRING,
    districtId STRING,
    birthDate STRING,
    updateTime BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
        WATERMARK FOR eventTime_ltz AS eventTime_ltz,
            PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'customers',
    'properties.bootstrap.servers' = 'redpanda:9092', --//'kafka:29092' <-- for kafka use this
    'key.format' = 'raw',
    'value.format' = 'json',
    'properties.group.id' = 'group.customers'
);

CREATE TABLE accounts (
    accountId STRING,
    districtId INT,
    frequency STRING,
    creationDate STRING,
    updateTime BIGINT,
    eventTime_ltz AS TO_TIMESTAMP_LTZ(updateTime, 3),
        WATERMARK FOR eventTime_ltz AS eventTime_ltz,
            PRIMARY KEY (accountId) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'accounts',
    'properties.bootstrap.servers' = 'redpanda:9092', --//'kafka:29092' <-- for kafka use this
    'key.format' = 'raw',
    'value.format' = 'json',
    'properties.group.id' = 'group.accounts'
);

SELECT
window_start AS windowStart,
window_end as windowEnd,
window_time AS windowTime,
COUNT(transactionId) as txnCount
FROM TABLE(
CUMULATE(
TABLE transactions,
DESCRIPTOR(eventTime_ltz),
 INTERVAL '2' HOUR,
 INTERVAL '1' DAY
 )
 )
 GROUP BY window_start, window_end, window_time
;

SELECT window_start AS windowStart, window_end AS windowEnd,window_time AS windowTime,COUNT(transactionId) AS txnCount
FROM   TABLE(
           TUMBLE(
               TABLE transactions,
               DESCRIPTOR(eventTime_ltz),
               INTERVAL '7' DAY
           )
)
GROUP BY window_start, window_end, window_time
;


