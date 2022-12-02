## Limitations

### Impala can't read schema evolution table with predicate on new column (Hive handles this without an error)

```sql
CREATE TABLE foo.bar (c1 INT) STORED AS ICEBERG;
INSERT INTO foo.bar VALUES (1);
ALTER TABLE foo.bar ADD COLUMN ts TIMESTAMP;
INSERT INTO foo.bar VALUES (2, "2022-01-01 00:00:00");
SELECT * FROM foo.bar WHERE ts >= "2022-01-01";
```

- Throws following error

```
Unable to find SchemaNode for path 'foo.bar.ts' in the schema of file 's3a://goes-se-sandbox01/warehouse/tablespace/external/hive/foo.db/bar/data/00101-0-data-hive_20221201122048_e2f79a77-e78a-4dc6-b528-6e9bc2e728dd-job_16698906862860_0009-8-00001.parquet'.
```

### Impala query plan always shows 1/1 partition scans from partitioned iceberg table

```sql
CREATE TABLE foo.bar (c1 INT) PARTITIONED BY SPEC (c1) STORED AS ICEBERG;
INSERT INTO foo.bar VALUES (1), (2), (3);
SELECT * FROM foo.bar WHERE c1 BETWEEN 1 AND 2;
```

- observed behaviour

```
00:SCAN S3 [foo.bar, RANDOM]
   S3 partitions=1/1 files=2 size=722B                          <-- 1/1 partitions even though table has 3 partitions, number of files and bytes scanned is correct
   predicates: c1 <= CAST(2 AS INT), c1 >= CAST(1 AS INT)
   stored statistics:
     table: rows=3 size=1.06KB
```

- expected behaviour: query plan shows 1/3 partitions scanned

## Hive in-place migration with timestamp type missing bounds in manifest files

- Observed behaviour

```sql
CREATE EXTERNAL TABLE foo.bar (t TIMESTAMP) STORED AS PARQUET;
INSERT INTO foo.bar VALUES ("2022-01-01 00:00:00"), ("2022-01-02 00:00:00"), ("2022-01-03 00:00:00");
ALTER TABLE foo.bar SET TBLPROPERTIES("storage_handler"="org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
```

- Produces manifest file with empty `lower_bounds` and `upper_bounds`

```json
{
    "status": 1,
    "snapshot_id": 6877669137313624000,
    "data_file": {
        "file_path": "s3a://goes-se-sandbox01/warehouse/tablespace/external/hive/foo.db/bar/000000_0",
        "file_format": "PARQUET",
        "partition": {},
        "record_count": 3,
        "file_size_in_bytes": 436,
        "block_size_in_bytes": 67108864,
        "column_sizes": [{"key": 1, "value": 76}],
        "value_counts": [{"key": 1, "value": 3}],
        "null_value_counts": [{"key": 1, "value": 0}],
        "nan_value_counts": [],
        "lower_bounds": [],
        "upper_bounds": [],
        "key_metadata": null,
        "split_offsets": null,
        "sort_order_id": 0
    }
}
```

- Expected behaviour

```sql
CREATE EXTERNAL TABLE foo.ice (t TIMESTAMP) STORED BY ICEBERG;
INSERT INTO foo.ice VALUES ("2022-01-01 00:00:00"), ("2022-01-02 00:00:00"), ("2022-01-03 00:00:00");
```

- Produces manifest file with populated `lower_bounds` and `upper_bounds`

```json
{
    "status": 1,
    "snapshot_id": 2079291146201653223, 
    "data_file": {
        "file_path": "s3a://goes-se-sandbox01/warehouse/tablespace/external/hive/foo.db/ice/data/00000-0-data-hive_20221202114909_ada4f17e-ede0-4e14-9032-79bccb0d831b-job_16699810810550_0001-1-00001.parquet",
        "file_format": "PARQUET",
        "partition": {},
        "record_count": 3,
        "file_size_in_bytes": 449,
        "block_size_in_bytes": 67108864,
        "column_sizes": [{"key": 1, "value": 55}],
        "value_counts": [{"key": 1, "value": 3}],
        "null_value_counts": [{"key": 1, "value": 0}],
        "nan_value_counts": [],
        "lower_bounds": [{"key": 1, "value": b'\x00`\xf9\xf7y\xd4\x05\x00'}],
        "upper_bounds": [{"key": 1, "value": b'\x00 \xa83\xa2\xd4\x05\x00'}],
        "key_metadata": "None",
        "split_offsets": [4],
        "sort_order_id": 0
    }
}
```

## Hive in-place migrated table inconsistent folder structure

- Observed behaviour 

```sql
CREATE EXTERNAL TABLE foo.bar (t TIMESTAMP) STORED AS PARQUET;
INSERT INTO foo.bar VALUES ("2022-01-01 00:00:00"), ("2022-01-02 00:00:00"), ("2022-01-03 00:00:00");
ALTER TABLE foo.bar SET TBLPROPERTIES("storage_handler"="org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
```

- Data files are written in `root/foo.db/bar` instead of `root/foo.db/bar/data`

```
$ aws s3 ls --recursive s3://goes-se-sandbox01/warehouse/tablespace/external/hive/foo.db/bar

2022-12-02 12:48:43        436 warehouse/tablespace/external/hive/foo.db/bar/000000_0
2022-12-02 12:48:46       1686 warehouse/tablespace/external/hive/foo.db/bar/metadata/00000-40c570bf-b9ee-4c54-b8b0-a7827da911dd.metadata.json
2022-12-02 12:48:47       2746 warehouse/tablespace/external/hive/foo.db/bar/metadata/00001-14495ab0-c487-4499-b815-c61af8260276.metadata.json
2022-12-02 12:48:47       5688 warehouse/tablespace/external/hive/foo.db/bar/metadata/6f6ba404-6843-4d23-9425-6f80161f20a5-m0.avro
2022-12-02 12:48:47       3783 warehouse/tablespace/external/hive/foo.db/bar/metadata/snap-6877669137313623974-1-6f6ba404-6843-4d23-9425-6f80161f20a5.avro
```
