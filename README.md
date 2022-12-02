# Iceberg workshop

- [Iceberg workshop](#iceberg-workshop)
  - [Goal](#goal)
  - [Setup](#setup)
    - [Prepared for you](#prepared-for-you)
    - [To be prepared by you](#to-be-prepared-by-you)
  - [Import: CTAS and in-place](#import-ctas-and-in-place)
  - [Time travel: Snapshots and rollback](#time-travel-snapshots-and-rollback)
  - [Partitioning](#partitioning)
    - [Partition evolution](#partition-evolution)
    - [Transform partitioning](#transform-partitioning)
    - [Predicate pushdown and file pruning without partitioning](#predicate-pushdown-and-file-pruning-without-partitioning)
  - [Maintenance: Metadata and data files](#maintenance-metadata-and-data-files)
    - [Metadata files](#metadata-files)
    - [Snapshot expiration](#snapshot-expiration)
    - [Optional maintenance](#optional-maintenance)
  - [Cleanup](#cleanup)

## Goal

- Internal workshop focussed on getting hands dirty with Iceberg on CDW and Iceberg v1 features.
- The workshop goes through the process of setting up a partitioned fact `flights` table and doing basic updates and maintenance on a dimension `airlines` table.

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
CREATE DATABASE IF NOT EXISTS ${user}_ice;
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

- Copy the flights table to avoid interference

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
```

- Use time travel to query the older snapshot

```sql
-- should not show any records
SELECT * FROM ${user}_ice.airlines
FOR SYSTEM_TIME AS OF "<snapshot-timestamp>"
WHERE code = "ABC";
```

- Rollback the table to before the insert

```sql
ALTER TABLE ${user}_ice.airlines
EXECUTE rollback("<snapshot-id>");

-- should not show any records
SELECT * FROM ${user}_ice.airlines
WHERE code = "ABC";
```

## Partitioning

### Partition evolution

- From **Impala** Virtual Warehouse
- Change the partitioning of the flights table to the year column

```sql
ALTER TABLE ${user}_ice.flights
SET PARTITION SPEC (year, month);
```

- Add new data with new partitioning schema (copy from 1995 as the year 2022)

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
  2022
FROM staging.flights_parquet
WHERE year = 1995;
```

- Verify (only) the new data matches the new partitioning schema

```
$ aws s3 ls --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/${user}_ice.db/flights/

...
# old data
2022-12-02 11:04:47   15216104 warehouse/tablespace/external/hive/mengel_ice.db/flights/000000_0
2022-12-02 11:04:47   13968217 warehouse/tablespace/external/hive/mengel_ice.db/flights/000001_0
2022-12-02 11:04:47   14498335 warehouse/tablespace/external/hive/mengel_ice.db/flights/000002_0
...
# new data
2022-12-02 11:32:45    5587934 warehouse/tablespace/external/hive/mengel_ice.db/flights/data/year=2022/month=1/c14aa210275935cf-5cd14a180000007b_76288533_data.0.parq
2022-12-02 11:32:45    4983949 warehouse/tablespace/external/hive/mengel_ice.db/flights/data/year=2022/month=2/c14aa210275935cf-5cd14a18000000c9_2017158224_data.0.parq
2022-12-02 11:32:45    5523667 warehouse/tablespace/external/hive/mengel_ice.db/flights/data/year=2022/month=3/c14aa210275935cf-5cd14a1800000085_99791295_data.0.parq
...
```

### Transform partitioning

- From **Impala** Virtual Warehouse
- Create a new table with transform partitioning

```sql
CREATE TABLE ${user}_ice.flights_p
PARTITIONED BY SPEC (year(ts))
STORED AS ICEBERG
AS SELECT *, cast(to_date(concat(cast(year AS STRING), "-", cast(month AS STRING), "-", cast(dayofmonth AS STRING))) AS TIMESTAMP) ts
FROM staging.flights_parquet;
```

- Query the transform partitioned table by any time derivative

```sql
SELECT count(*)
FROM ${user}_ice.flights_p
WHERE ts BETWEEN "2008-01-01" AND "2008-12-31";

SELECT count(*)
FROM ${user}_ice.flights_p
WHERE ts = "2008-01-01 00:00:00";

SELECT count(*)
FROM ${user}_ice.flights_p
WHERE year = 2008;
```

- The query plan should show that for both queries only one file has been scanned

```
...
00:SCAN S3 [mengel_ice.flights_p, RANDOM]
   S3 partitions=1/1 files=1 size=131.45MB              <-- 1 file and 131MB out of 1.28GB are scanned
   predicates: ts <= TIMESTAMP '2008-12-31 00:00:00', ts >= TIMESTAMP '2008-01-01 00:00:00'
   stored statistics:
     table: rows=86.29M size=1.28GB
...
```

- **Optional**: Query the non-partitioned source table 

```sql
SELECT count(*)
FROM staging.flights_parquet
WHERE year = 2008;
```

- The query plan should show that all files have been scanned

```
...
00:SCAN S3 [staging.flights_parquet, RANDOM]
   S3 partitions=1/1 files=238 size=1.28G             <-- all files and 1.28GB out of 1.28GB are scanned
   predicates: `year` = CAST(2008 AS INT)
   stored statistics:
     table: rows=86.29M size=1.28GB
...
```

### Predicate pushdown and file pruning without partitioning

- Query engines can utilize Iceberg metadata and predicate pushdown to avoid scanning unnecessary files
- This is also the case for non-partitioned tables
- Create an Iceberg table without partitioning

```sql
CREATE TABLE ${user}_ice.flights_np
STORED AS ICEBERG
AS SELECT *
FROM staging.flights_parquet;
```

- Query the non-partitioned Iceberg table with a predicate

```sql
SELECT count(*)
FROM ${user}_ice.flights_np
WHERE year = 2008;
```

- The query plan shows that a subset of files have been scanned

```
...
00:SCAN S3 [mengel_ice.flights_np, RANDOM]
   S3 partitions=1/1 files=20 size=220.48MB             <-- 20 files and 220MB out of 1.16GB are scanned
   predicates: `year` = CAST(2008 AS INT)
   stored statistics:
     table: rows=86.29M size=1.16GB
...
```

## Maintenance: Metadata and data files

Suggested in the [official Iceberg docs](https://iceberg.apache.org/docs/latest/maintenance/#recommended-maintenance) are:

- [Expire Snapshots](#snapshot-expiration)
- [Remove old metadata files](#metadata-files)

All SQL below is to be executed from the **Hive** Virtual Warehouse.

### Metadata files

- A new metadata file is created with every commit and schema change to the table
- List all currently stored metadata files (should list multiple metadata files)

```bash
$ aws s3 ls --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/mengelhardt_ice.db/airlines/metadata
```

- Set `previous-versions-max` to 1 to only keep the latest metadata file (not for production)
- Enable `delete-after-commit` to allow query engines to delete old metadata files after every commit

```sql
ALTER TABLE ${user}_ice.airlines
SET TBLPROPERTIES(
    "write.metadata.previous-versions-max"="1",
    "write.metadata.delete-after-commit.enabled"="true");

-- make a commit to the table by deleting records
TRUNCATE TABLE ${user}_ice.airlines;
```

- Verify old metadata files were removed

```bash
$ aws s3 ls --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/${user}_ice.db/airlines/metadata
```

- **Optional**: Deeper look into the updated `write.metadata` properties

```bash
$ aws s3 cp --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/${user}_ice.db/airlines/metadata ./iceberg-metadata
$ cat ./iceberg-metadata/metadata/<hash>.metadata.json | grep write.metadata

    "write.metadata.previous-versions-max" : "1",
    "write.metadata.delete-after-commit.enabled" : "true",
```

### Snapshot expiration

- Using snapshot expiration can help with "housekeeping" old snapshots and referenced data files
- Recall we just deleted all records with the `TRUNCATE TABLE` statement
- Now, insert some data to create a new snapshot with new data files

```sql
INSERT INTO ${user}_ice.airlines
VALUES("ABC", "Real Fake Airlines");
```

- Get the timestamp of the **latest snapshot**

```
SELECT * FROM ${user}_ice.airlines.history;
```

| airlines.made_current_at | airlines.snapshot_id	| airlines.parent_id | airlines.is_current_ancestor |
|---|---|---|---|
| 2022-11-28 11:00:59.118 Z |	6517620803989910949 |	NULL |	true |
| 2022-11-28 11:01:03.938 Z |	1873082738288129896 |	6517620803989910949 |	false |
| 2022-11-28 11:01:13.03 Z |	6517620803989910949 |	NULL |	true |
| 2022-11-28 11:01:20.083 Z |	5046963992640638903 |	6517620803989910949 |	true |
| **2022-11-28 11:01:25.63 Z** |	7812932059031611813 |	5046963992640638903 |	true |

- Expire all snapshots older than the current one
- This will create orphaned data files (data files that are no longer references by any snapshot), which are cleaned up in the process

```sql
ALTER TABLE ${user}_ice.airlines
EXECUTE expire_snapshots("<snapshot-timestamp>");
```

- Verify that all snapshots are deleted but the current one

```sql
SELECT * FROM ${user}_ice.airlines.history;
```

- Verify that orphaned data files have been deleted as well (should show only one data file with one record)

```bash
$ aws s3 ls --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/${user}_ice.db/airlines/
```

- **Optional**: Verify the data file only contains one record

```bash
$ aws s3 cp --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/${user}_ice.db/airlines/data ./data
$ parquet-tools show data/<data-file-name>.parquet
+--------+--------------------+
| code   | description        |
|--------+--------------------|
| ABC    | Real Fake Airlines |
+--------+--------------------+
```

### Optional maintenance

Suggested in the [official Iceberg docs](https://iceberg.apache.org/docs/latest/maintenance/#optional-maintenance) are also:

- Compact data files (supported only from Spark at the moment)

```sql
CALL catalog_name.system.rewrite_data_files('db.sample')
```

- Rewrite manifests (supported only from Spark)

```sql
CALL catalog_name.system.rewrite_manifests('db.sample')
```

## Cleanup

```sql
DROP DATABASE ${user}_ice CASCADE;
```
