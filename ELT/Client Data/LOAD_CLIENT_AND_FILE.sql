INSERT INTO CUSTOMER.MANAGEMENT.CLIENT(ABBREVIATION, NAME, NOTES)
VALUES ('IFLY', 'Inferlytica', 'Internal Client - Testing');

SELECT *
FROM CUSTOMER.MANAGEMENT.CLIENT;
--1

INSERT INTO CUSTOMER.FILE_PROCESSING.CLIENT_BATCH(CLIENT_ID, FILE_NAME, RECORD_COUNT, NOTES)
VALUES(1, 'updated_sample.csv.gz', 100, 'Test File');

DESC TABLE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;
--1

DESC TABLE CUSTOMER.RAW.PERSON_INPUT;
--SELECT t.$1, t.$2 FROM @mystage1 (file_format => 'myformat', pattern=>'.*data.*[.]csv.gz') t;

USE DATABASE CUSTOMER;
LIST @CUSTOMER_NAMES;

INSERT INTO CUSTOMER.RAW.PERSON_INPUT(CLIENT_ID, BATCH_ID,RECORD_ID, FIRST_NAME, LAST_NAME, ZIP)
SELECT 1, 101, t.$1,  t.$2, t.$3, t.$4 FROM @CUSTOMER_NAMES (pattern=>'updated_sample.csv.gz') t;
DELETE
FROM CUSTOMER.RAW.PERSON_INPUT;

REMOVE @CUSTOMER_NAMES;
-- Refresh dynamic to pull data downstream
alter dynamic table CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE refresh;


-- SELECT name, database_name, schema_name, data_timestamp, state, state_code, state_message, refresh_trigger
-- FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
-- WHERE name = 'FIRST_NAME_GENDER_QUANTITY_YEAR'
--   AND database_name = 'DATA_PROVIDER_SSA'
-- ORDER BY data_timestamp DESC
-- LIMIT 10;

SQL compilation error: Change tracking is not enabled or has been missing for the time range requested on table 'DATA_PROVIDER_SSA.RAW.FIRST_NAME_GENDER_QUANTITY_YEAR'.

-- Step 1: snow connection test
-- Step 2: snow sql
-- Step 3: USE DATABASE CUSTOMER;

-- If Stage & File Format is not yet created
-- Step 3a: CREATE OR REPLACE FILE FORMAT CUSTOMER_NAMES_CSV TYPE=CSV SKIP_HEADER=1 COMMENT='For Client Names File';
-- Step 3b: CREATE STAGE CUSTOMER_NAMES FILE_FORMAT = CUSTOMER_NAMES_CSV;

-- Step 4: Make sure files aren't currently in Stage: LIST @CUSTOMER_NAMES;
-- Step 4a: If files are in Stage: REMOVE @CUSTOMER_NAMES;

-- Step 5: Using actual local path: PUT file://C:\Users\Charles\Documents\Inferlytica\test_client_data\updated_sample.csv @CUSTOMER_NAMES;
-- Step 6: COPY INTO CUSTOMER.RAW.FIRST_NAME_GENDER_QUANTITY_YEAR FROM @CUSTOMER_NAMES;
-- Step 7: Verify Row count appears accurate based on results in Step 6 (empty rows are skipped I think)
-- Step 8: Clean up Stage: REMOVE @CUSTOMER_NAMES;
-- Step 9: Force downstream refresh: alter dynamic table CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE refresh;