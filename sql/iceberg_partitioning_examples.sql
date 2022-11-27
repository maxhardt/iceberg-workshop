-- cleanup
DROP DATABASE ice_p CASCADE;

-- database
CREATE DATABASE ice_p;

-- iceberg table with identity partitioning
CREATE EXTERNAL TABLE ice_p.orders_ip (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount FLOAT,
    order_ts TIMESTAMP
)
PARTITIONED BY (order_date DATE)
STORED BY ICEBERG
STORED AS PARQUET;

-- inserting data into table with identity partitioning
INSERT INTO ice_p.orders_ip
VALUES (1, 1, 100.0, "2022-01-01 00:00:00", "2022-01-01"),
       (1, 1, 100.0, "2022-01-02 00:00:00", "2022-01-02");

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

INSERT INTO ice_p.orders_ip
SELECT *, to_date(order_ts) FROM orders_tmp;
  
SELECT * FROM ice_p.orders_ip;

-- iceberg table with transform partitioning
CREATE EXTERNAL TABLE ice_p.orders_tp (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount FLOAT,
    order_ts TIMESTAMP
)
PARTITIONED BY SPEC (DAYS(order_ts))
STORED BY ICEBERG
STORED AS PARQUET;

-- inserting data into table with advanved partitioning
INSERT INTO ice_p.orders_tp
VALUES (1, 1, 100.0, "2022-01-01 00:00:00"),
       (1, 1, 100.0, "2022-01-02 00:00:00");

INSERT INTO ice_p.orders_tp
SELECT * FROM staging.orders_tmp;

SELECT * FROM ice_p.orders_tp;

-- query iceberg partitioned data
-- from impala to see execution plan

SELECT * FROM ice_p.orders_tp
WHERE order_ts BETWEEN "2022-01-01 00:00:00" AND "2022-01-01 18:00:00";
