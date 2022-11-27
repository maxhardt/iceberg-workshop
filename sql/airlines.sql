DROP DATABASE IF EXISTS staging CASCADE;

CREATE DATABASE staging;

CREATE EXTERNAL TABLE staging.flights_csv (
  month INT,
  dayofmonth INT,
  dayofweek INT,
  deptime INT,
  crsdeptime INT,
  arrtime INT,
  crsarrtime INT,
  uniquecarrier STRING,
  flightnum INT,
  tailnum STRING,
  actualelapsedtime INT, 
  crselapsedtime INT,
  airtime INT,
  arrdelay INT,
  depdelay INT,
  origin STRING,
  dest STRING,
  distance INT,
  taxiin INT,
  taxiout INT,
  cancelled INT,
  cancellationcode STRING,
  diverted STRING,
  carrierdelay INT,
  weatherdelay INT,
  nasdelay INT,
  securitydelay INT,
  lateaircraftdelay INT,
  year INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3a://${bucket}/tmp/airlines-csv/flights/'
TBLPROPERTIES("skip.header.line.count"="1");

CREATE EXTERNAL TABLE staging.airlines_csv (
  code STRING,
  description STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION 's3a://${bucket}/tmp/airlines-csv/airlines/'
TBLPROPERTIES("skip.header.line.count"="1");

CREATE EXTERNAL TABLE staging.flights_parquet
STORED AS PARQUET
AS SELECT * FROM staging.flights_csv;

CREATE EXTERNAL TABLE staging.airlines_parquet
STORED AS PARQUET
AS SELECT * FROM staging.airlines_csv;
