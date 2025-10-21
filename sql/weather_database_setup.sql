

CREATE DATABASE weather;

CREATE OR REPLACE TABLE weather_update (
forecast_date DATE,         -- NEW: Maps to the YYYY-MM-DD part ($1)
forecast_time TIME,         -- NEW: Maps to the HH:MM:SS part ($2)
temperature_2m FLOAT,       -- Maps to $3
humidity_2m INT,            -- Maps to $4
windspeed_10m FLOAT         -- Maps to $5
);

SELECT * FROM weather_update;

DROP FILE FORMAT my_csv_format;

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE STORAGE INTEGRATION etl_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::177603749650:role/Lambda-Glue-S3-Role'
STORAGE_ALLOWED_LOCATIONS = ('s3://my-etldata-bucket/load/');

DESC INTEGRATION etl_integration;

GRANT USAGE ON INTEGRATION etl_integration TO ROLE SYSADMIN;

DROP STAGE weather_stage;

CREATE OR REPLACE STAGE weather_stage
URL = 's3://my-etldata-bucket/load/'
CREDENTIALS = (AWS_KEY_ID = ''
AWS_SECRET_KEY = '')
FILE_FORMAT = my_csv_format;

LIST @weather_stage;
SHOW STAGES;
LIST @weather_stage;

DESC PIPE weather_s3_pipe;

CREATE OR REPLACE PIPE weather_s3_pipe
AUTO_INGEST = TRUE
AS
-- MAPPING FIVE COLUMNS: Ensure the order matches the Lambda output
COPY INTO weather_update (forecast_date, forecast_time, temperature_2m, humidity_2m, windspeed_10m)
FROM (SELECT
$1, -- Column 1 in CSV (forecast_date)
$2, -- Column 2 in CSV (forecast_time)
$3, -- Column 3 in CSV (temperature_2m)
$4, -- Column 4 in CSV (humidity_2m)
$5  -- Column 5 in CSV (windspeed_10m)
FROM @weather_stage)
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

ALTER PIPE weather_s3_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
ALTER PIPE weather_s3_pipe REFRESH;
SELECT SYSTEM$PIPE_STATUS('weather_s3_pipe');

SELECT
FILE_NAME,
STATUS,
ROW_COUNT,
FIRST_ERROR_MESSAGE,
LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
TABLE_NAME=>'WEATHER_UPDATE',
START_TIME=>DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

SELECT COUNT(*) FROM WEATHER.PUBLIC.weather_update;

SELECT * FROM weather_update;

