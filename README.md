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

Suggested in the [official Iceberg docs](https://iceberg.apache.org/docs/latest/maintenance/#recommended-maintenance) are:

- [Expire Snapshots](#snapshot-expiration)
- [Remove old metadata files](#metadata-files)

All SQL below is to be executed from the **Hive** Virtual Warehouse.

### Metadata files

- A new metadata file is created with every commit and schema change to the table
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
- Delete all records to create a new snapshot and orphaned data files

```sql
TRUNCATE TABLE ${user}_ice.airlines;
```

- Insert some data to create a new snapshot with new data files

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
