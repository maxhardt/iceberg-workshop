# Testing Interoparability of CDP Iceberg with external Spark

- Even though both engines specify Iceberg v1 spec the produced metadata strucutre is different.
- Attempting to read CDW Hive produced Iceberg table from Spark throws an error.

## Iceberg v1 Iceberg table created from CDW Hive

### Setup

- From CDW Hive Virtual Warehouse with Iceberg support

### Create table via CDW Hive

```sql
create external table ice.flights_ice_v1
stored by iceberg
stored as parquet
tblproperties ('format-version' = '1')
as select * from staging.flights_parquet
```

- Produces following directory structure

```bash
(venv-avro-cli) ➜  iceberg-demo aws s3 ls --recursive s3://mengel-uat1/warehouse/tablespace/external/hive/ice.db/flights_ice_v1/metadata
2022-11-26 15:00:29       6970 warehouse/tablespace/external/hive/ice.db/flights_ice_v1/metadata/00000-79290f2a-d7cb-4ef6-bf90-afd5a18a7fe0.metadata.json
2022-11-26 15:00:45       8072 warehouse/tablespace/external/hive/ice.db/flights_ice_v1/metadata/00001-88c878dc-a897-4cc8-b1c8-36fa9b0f290f.metadata.json
2022-11-26 15:00:45      35539 warehouse/tablespace/external/hive/ice.db/flights_ice_v1/metadata/0fc7fdc3-c7e3-465b-acd1-3f8458f91e09-m0.avro
2022-11-26 15:00:45       3800 warehouse/tablespace/external/hive/ice.db/flights_ice_v1/metadata/snap-8519861019640010815-1-0fc7fdc3-c7e3-465b-acd1-3f8458f91e09.avro
```

## Iceberg v1 table created from local Docker Spark

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

### Create table via Docker Spark

- Setup a `HadoopCatalog` with S3 warehouse directory

```python
spark.conf.set("spark.sql.catalog.hadoop_s3", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.hadoop_s3.type", "hadoop")
spark.conf.set("spark.sql.catalog.hadoop_s3.warehouse", "s3a://mengel-uat1/tmp/spark")
```

- CTAS table `flights_ice` with data from existing parquet file

```python
flights_parquet = spark.read.parquet("s3a://mengel-uat1/warehouse/tablespace/external/hive/staging.db/flights_parquet")
flights_parquet.createOrReplaceTmpView("flights_parquet")
spark.sql("CREATE DATABASE hadoop_s3.flights")
spark.sql("CREATE TABLE hadoop_s3.flights.flights_ice AS SELECT * FROM flights_parquet")
```

- Produces following directory structure

```bash
(venv-avro-cli) ➜  iceberg-demo aws s3 ls --recursive s3://mengel-uat1/tmp/spark/flights/flights_ice/metadata
2022-11-26 12:44:22      13451 tmp/spark/flights/flights_ice/metadata/3d1d4447-a144-44a1-a046-dacf7c5f5505-m0.avro
2022-11-26 12:44:24       3767 tmp/spark/flights/flights_ice/metadata/snap-1836139004531383525-1-3d1d4447-a144-44a1-a046-dacf7c5f5505.avro
2022-11-26 12:44:27       7625 tmp/spark/flights/flights_ice/metadata/v1.metadata.json
2022-11-26 12:44:30          1 tmp/spark/flights/flights_ice/metadata/version-hint.text
```

# Attempting to read CDW created table from local Spark

- Throws following error looking for the `version-hint.text` file

```python
>>> spark.read.format("iceberg").load("s3a://mengel-uat1/warehouse/tablespace/external/hive/ice.db/flights_ice_v1")
22/11/26 14:10:01 WARN HadoopTableOperations: Error reading version hint file s3a://mengel-uat1/warehouse/tablespace/external/hive/ice.db/flights_ice_v1/metadata/version-hint.text
java.io.FileNotFoundException: No such file or directory: s3a://mengel-uat1/warehouse/tablespace/external/hive/ice.db/flights_ice_v1/metadata/version-hint.text
```
