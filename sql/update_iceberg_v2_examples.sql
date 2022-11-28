-- cleanup
DROP DATABASE updates_ice CASCADE;

-- database
CREATE DATABASE updates_ice;

-- create an iceberg table
CREATE EXTERNAL TABLE updates_ice.airlines
STORED BY ICEBERG
STORED AS PARQUET
AS SELECT * FROM staging.airlines_parquet;

-- perform an update on the iceberg table
MERGE INTO updates_ice.airlines AS target
USING (SELECT code, description FROM staging.airlines_parquet WHERE code = "02Q") AS source
ON code = source.code
WHEN MATCHED THEN UPDATE SET code=source.code, description="Titanic Trauma"
WHEN NOT MATCHED THEN INSERT VALUES (source.code, "Titanic Trauma");
