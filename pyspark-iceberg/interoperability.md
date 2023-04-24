# Testing Interoparability of CDP Iceberg with external Spark

- Within Iceberg v1 spec the resulting directory structure and naming convention are slightly different.
- Read(-only) access from external Spark is possible by specifying the full filepath to the metadata location
- For full integration with CDP Spark needs to register a `hive` type catalog against the CDP HMS (not fully tested)

- [Testing Interoparability of CDP Iceberg with external Spark](#testing-interoparability-of-cdp-iceberg-with-external-spark)
  - [Creating an Iceberg v1 table from CDW Hive Virtual Warehouse](#creating-an-iceberg-v1-table-from-cdw-hive-virtual-warehouse)
  - [Creating an Iceberg v1 table created from local Spark on Docker](#creating-an-iceberg-v1-table-created-from-local-spark-on-docker)
  - [Reading an Iceberg table created in CDW from local Spark on Docker](#reading-an-iceberg-table-created-in-cdw-from-local-spark-on-docker)
  - [Full integration of CDP Iceberg tables and local Spark on Docker via `hive` type catalog against CDP HMS](#full-integration-of-cdp-iceberg-tables-and-local-spark-on-docker-via-hive-type-catalog-against-cdp-hms)

## Creating an Iceberg v1 table from CDW Hive Virtual Warehouse

```sql
-- create iceberg v1 table
CREATE EXTERNAL TABLE mengel.ice (c1 int)
STORED BY ICEBERG
TBLPROPERTIES ('format-version' = '1');

-- insert some data
INSERT INTO mengel.ice VALUES(1);
```

- Produces following directory structure

```bash
(venv-avro-cli) ➜  iceberg-demo aws s3 ls --recursive s3://goes-se-sandbox01/warehouse/tablespace/external/hive/mengel.db/ice/metadata
2022-11-26 15:00:29       6970 warehouse/tablespace/external/hive/mengel.db/ice/metadata/00000-79290f2a-d7cb-4ef6-bf90-afd5a18a7fe0.metadata.json
2022-11-26 15:00:45       8072 warehouse/tablespace/external/hive/mengel.db/ice/metadata/00001-88c878dc-a897-4cc8-b1c8-36fa9b0f290f.metadata.json
2022-11-26 15:00:45      35539 warehouse/tablespace/external/hive/mengel.db/ice/metadata/0fc7fdc3-c7e3-465b-acd1-3f8458f91e09-m0.avro
2022-11-26 15:00:45       3800 warehouse/tablespace/external/hive/mengel.db/ice/metadata/snap-8519861019640010815-1-0fc7fdc3-c7e3-465b-acd1-3f8458f91e09.avro
```

## Creating an Iceberg v1 table created from local Spark on Docker

### Setup

- Download Spark Iceberg docker compose file from https://iceberg.apache.org/spark-quickstart/
- Start Pyspark session with Hadoop S3 dependencies

```bash
docker compose up
docker exec -it spark-iceberg pyspark --packages org.apache.spark:spark-hadoop-cloud_2.12:3.3.1
```

- Configure S3 credentials in Pyspark

```python
>>> spark._sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
>>> spark._sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
>>> spark._sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
```

### Creating an Iceberg table in a `hadoop` type catalog on S3

- Setup a `hadoop` type Iceberg catalog with S3 warehouse directory

```python
>>> spark.conf.set("spark.sql.catalog.hadoop_s3", "org.apache.iceberg.spark.SparkCatalog")
>>> spark.conf.set("spark.sql.catalog.hadoop_s3.type", "hadoop")
>>> spark.conf.set("spark.sql.catalog.hadoop_s3.warehouse", "s3a://goes-se-sandbox/tmp/mengel/hadoop-spark-catalog")
```

- CTAS a new table `ice` in database `mengel`

```python
>>> spark.sql("CREATE DATABASE hadoop_s3.mengel")
>>> spark.sql("CREATE TABLE hadoop_s3.mengel.ice (c1 int) USING ICEBERG")
>>> spark.sql("INSERT INTO hadoop_s3.mengel.ice VALUES (1)")
```

- Produces following directory structure

```bash
$ aws s3 ls --recursive s3://goes-se-sandbox01/tmp/mengel/spark-hadoop-catalog/mengel/ice/
2022-12-09 13:15:00        401 tmp/mengel/spark-hadoop-catalog/mengel/ice/data/00000-2-ab7d8ec5-926f-4220-ba1d-3e26689a6d60-00001.parquet
2022-12-09 13:15:02       5732 tmp/mengel/spark-hadoop-catalog/mengel/ice/metadata/67640563-12ff-4ecc-83d7-02c190e7d660-m0.avro
2022-12-09 13:15:03       3779 tmp/mengel/spark-hadoop-catalog/mengel/ice/metadata/snap-1921426768911195856-1-67640563-12ff-4ecc-83d7-02c190e7d660.avro
2022-12-09 13:14:42       1021 tmp/mengel/spark-hadoop-catalog/mengel/ice/metadata/v1.metadata.json
2022-12-09 13:15:07       2081 tmp/mengel/spark-hadoop-catalog/mengel/ice/metadata/v2.metadata.json
2022-12-09 13:15:10          1 tmp/mengel/spark-hadoop-catalog/mengel/ice/metadata/version-hint.text
```

## Reading an Iceberg table created in CDW from local Spark on Docker

- Retrieve the metadata location from CDW

```sql
DESCRIBE FORMATTED mengel.ice;
/*
...
metadata_location

s3a://goes-se-sandbox01/warehouse/tablespace/external/hive/mengel.db/ice/metadata/00001-217983c6-b390-473e-9be6-884f4655a507.metadata.json
...
*/
```

- Read the table in local Spark on Docker by specifying the full filepath to the metadata file

```python
>>> spark.read.format("iceberg").load("s3a://goes-se-sandbox01/warehouse/tablespace/external/hive/mengel.db/ice/metadata/00001-217983c6-b390-473e-9be6-884f4655a507.metadata.json")
```

- **NOTE**: Full filepath to the metadata file must be specified, otherwise Spark throws an error because it expects a differnet file structure & naming convention

```python
>>> spark.read.format("iceberg").load("s3a://goes-se-sandbox01/warehouse/tablespace/external/hive/mengel.db/ice")
22/11/26 14:10:01 WARN HadoopTableOperations: Error reading version hint file s3a://goes-se-sandbox01/warehouse/tablespace/external/hive/mengel.db/ice/metadata/version-hint.text
java.io.FileNotFoundException: No such file or directory: s3a://goes-se-sandbox01/warehouse/tablespace/external/hive/mengel.db/ice/metadata/version-hint.text
```

## Full integration of CDP Iceberg tables and local Spark on Docker via `hive` type catalog against CDP HMS

- For full integration an Iceberg catalog that points to the CDP HMS is needed
- This should, in theory, also allow updating or deleting the table, which can then be read again by CDP services, e.g. CDW

```
>>> spark.conf.set("spark.sql.catalog.hive_cdp", "org.apache.iceberg.spark.SparkCatalog")
>>> spark.conf.set("spark.sql.catalog.hive_cdp.type", "hive")
>>> spark.conf.set("spark.sql.catalog.hive_cdp.uri", "thrift://se-aw-mdl-master0.se-sandb.a465-9q4k.cloudera.site:9083")
```

- Modifying an existing table, e.g. created by CDW (**not tested yet**)

```python
>>> spark.sql("DELETE FROM hive_cdp.mengel.ice WHERE c1 = 1")
```
