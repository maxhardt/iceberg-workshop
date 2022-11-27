-- cleanup
DROP DATABASE hive_p CASCADE;

-- database
CREATE DATABASE hive_p;

-- hive partitioned table
CREATE EXTERNAL TABLE hive_p.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount FLOAT,
    order_ts TIMESTAMP
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET;

-- inserting data with dynamic partitioning
-- hive.exec.dynamic.partition=true
-- hive.exec.dynamic.partition.mode=nonstrict

INSERT INTO hive_p.orders
PARTITION(order_date="2022-01-01")
VALUES (1, 1, 100.0, "2022-01-01 00:00:00");

INSERT INTO hive_p.orders
VALUES (1, 1, 100.0, "2022-01-02 00:00:00", "2022-01-02");

-- create dummy source table
DROP TABLE IF EXISTS orders_tmp;
CREATE TABLE orders_tmp (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount FLOAT,
    order_ts TIMESTAMP
);
INSERT INTO orders_tmp
VALUES (1, 1, 100.0, "2022-01-03 00:00:00");

INSERT INTO hive_p.orders
PARTITION(order_date)
SELECT *, to_date(order_ts) FROM orders_tmp;

-- verify partitions are created
SHOW PARTITIONS hive_p.orders;

-- query hive partitioned data
-- from impala to see execution pla

SELECT * FROM hive_p.orders
WHERE order_ts BETWEEN "2022-01-01 00:00:00" AND "2022-01-01 18:00:00"

SELECT * FROM hive_p.orders
WHERE order_ts BETWEEN "2022-01-01 00:00:00" AND "2022-01-01 18:00:00"
AND order_date = "2022-01-01";
