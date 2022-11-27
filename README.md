# Iceberg workshop

- [Iceberg workshop](#iceberg-workshop)
  - [Goal](#goal)
  - [Setup](#setup)
    - [Prepared for you](#prepared-for-you)
    - [To be prepared by you](#to-be-prepared-by-you)
  - [Import: CTAS and in-place](#import-ctas-and-in-place)
  - [Time travel: Snapshots and rollback](#time-travel-snapshots-and-rollback)
  - [Schema Evolution and (hidden) partitioning](#schema-evolution-and-hidden-partitioning)
  - [Maintenance: Metadata and data files](#maintenance-metadata-and-data-files)
    - [Metadata files](#metadata-files)
    - [Snapshot expiration](#snapshot-expiration)
    - [Data file compaction](#data-file-compaction)
  - [Cleanup](#cleanup)

## Goal

- Internal workshop focussed on getting hands dirty with Iceberg on CDW and Iceberg v1 features.
- The goal is to set up a partitioned fact `flights` and dimension `airlines` table in iceberg for analytics.

## Setup

### Prepared for you

- A CDP Environment and Data Lake
- A CDW Hive Virtual Warehouse
- A CDW Impala Virtual Warehouse
- The database `staging` containing data for the workshop

```sql
SHOW TABLES IN staging;
```

|tab_name|
|---|
|airlines_csv|
|airlines_parquet|
|flights_csv|
|flights_parquet|

### To be prepared by you

- Connect to the Hive Virtual Warehouse
- Create a database with your name as prefix

```sql
CREATE DATABASE IF NOT EXISTS ${name}_ice;
```

- *Optional*: Make sure you can use the `aws` cli for tracking changes to metadata and data files for the `aws-se-cdp-sandbox-env` account

```bash
$ aws configure
$ aws s3 ls s3://mengel-uat1/warehouse/tablespace/external/hive/staging.db --recursive
```

- *Optional*: Install avro cli and parquet-tools for inspecting binaries

```bash
python3 -m venv ./venv-ice
source ./venv-ice
pip3 install --upgrade pip avro parquet-tools
```

## Import: CTAS and in-place

- From **Hive** Virtual Warehouse
- Import the airlines table into Iceberg using CTAS

```sql
CREATE EXTERNAL TABLE ${user}_ice.airlines
STORED BY ICEBERG
STORED AS PARQUET
AS SELECT * FROM staging.airlines_parquet;
```

- Copy the flights table to avoid interference (don't do this outside of the workshop lol)

```sql
CREATE EXTERNAL TABLE ${user}_ice.flights
STORED AS PARQUET
AS SELECT * FROM staging.flights_parquet;
```

- Import the (larger) flights table into Iceberg using in-place migration

```sql
ALTER TABLE ${user}_ice.flights
SET TBLPROPERTIES("storage_handler"="org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
```

## Time travel: Snapshots and rollback

- From **Hive** Virtual Warehouse
- Insert a fake airline into the airlines table

```sql
INSERT INTO ${user}_ice.airlines
VALUES ("ABC", "Real Fake Airlines");

-- should return 1 record
SELECT * FROM ${user}_ice.airlines
WHERE code = "ABC";

-- should not show any records
SELECT * FROM ${user}_ice.airlines
FOR SYSTEM_TIME AS OF "2022-11-29 00:00:00"
WHERE code = "ABC";
```

- Rollback the table to before the insert

```sql
ALTER TABLE mengelhardt_ice.airlines
EXECUTE rollback("2022-11-27 20:00:00");

-- should not show any records
SELECT * FROM mengelhardt_ice.airlines
WHERE code = "ABC";
```

## Schema Evolution and (hidden) partitioning

- From **Hive** Virtual Warehouse
- Add a timestamp column

```sql
ALTER TABLE ${user}_ice.flights
ADD COLUMNS (ts TIMESTAMP);
```

- Set partitioning to year transformed from the new timestamp column

```sql
ALTER TABLE ${user}_ice.flights
SET PARTITION SPEC (year(ts));
```

- Add fake data from 1995 for year 2022

```sql
INSERT INTO ${user}_ice.flights
SELECT
  month,
  dayofmonth,
  dayofweek,
  deptime,
  crsdeptime,
  arrtime,
  crsarrtime,
  uniquecarrier,
  flightnum,
  tailnum,
  actualelapsedtime,
  crselapsedtime,
  airtime,
  arrdelay,
  depdelay,
  origin,
  dest,
  distance,
  taxiin,
  taxiout,
  cancelled,
  cancellationcode,
  diverted,
  carrierdelay,
  weatherdelay,
  nasdelay,
  securitydelay,
  lateaircraftdelay,
  2022,
  cast(to_date(concat("2022", "-", month, "-", dayofmonth)) AS TIMESTAMP) ts
FROM staging.flights_parquet
WHERE year = 1995;
```

- From **Impala** Virtual Warehouse

- Query the table for the "new" data on any time derivative and verify only the partition has been scanned

```sql
SELECT * FROM ${user}_ice.flights
WHERE to_date(ts) >= "2022-01-01";
```

- Query the table for the "old" data on any time derivative and verify all non-partitioned data files were scanned

```sql
SELECT * FROM ${user}_ice.flights
WHERE year < 2022;
```

## Maintenance: Metadata and data files

### Metadata files

```sql
ALTER TABLE mengelhardt_ice.airlines
SET TBLPROPERTIES(
    "write.metadata.previous-versions-max"="1",
    "write.metadata.delete-after-commit.enabled"="true");

-- make a commit to the table by writing to it
INSERT INTO mengelhardt_ice.airlines
VALUES("ABC", "Real Fake Airlines");
```

- Verify old metadata files were removed

```bash
$ aws s3 cp --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/${user}_ice.db/airlines/metadata ./iceberg-metadata
```

- Verify settings show up in the latest metadata file

```
$ cat ./iceberg-metadata/metadata/<hash>.metadata.json | grep write.metadata

    "write.metadata.previous-versions-max" : "1",
    "write.metadata.delete-after-commit.enabled" : "true",
```

### Snapshot expiration

... #TODO

### Data file compaction

... #TODO

## Cleanup

```sql
DROP DATABASE ${user}_ice CASCADE;
```
