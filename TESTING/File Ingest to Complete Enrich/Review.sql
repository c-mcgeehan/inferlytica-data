
--did ingestion pipe run? 
SELECT SYSTEM$PIPE_STATUS('CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE');
-- {"executionState":"RUNNING","pendingFileCount":0,"lastIngestedTimestamp":"2026-04-07T11:52:47.611Z","lastIngestedFilePath":"client_1d96f728-bdc7-4554-8167-89cbd072b075/batch_8ba86a47-2574-4aab-8ecb-89026eb8e72c/2e98271d-b842-4952-999f-2ec5362c88ce_10if_test_updated_sample.csv","notificationChannelName":"arn:aws:sqs:us-east-1:004878718171:sf-snowpipe-AIDAQCIWLKTNUANPOWPIV-Ym7zU2p6AX2KU7RyzdJdDg","numOutstandingMessagesOnChannel":1,"lastReceivedMessageTimestamp":"2026-04-07T11:52:47.369Z","lastForwardedMessageTimestamp":"2026-04-07T11:52:47.838Z","lastPulledFromChannelTimestamp":"2026-04-07T11:54:52.39Z","lastForwardedFilePath":"inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/client_1d96f728-bdc7-4554-8167-89cbd072b075/batch_8ba86a47-2574-4aab-8ecb-89026eb8e72c/2e98271d-b842-4952-999f-2ec5362c88ce_10if_test_updated_sample.csv"}
-- check the last ingest time, check the file name


--check if data loaded okay
SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED',
    START_TIME => DATEADD('hour', -2, CURRENT_TIMESTAMP()),
    PIPE_NAME => 'CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE'
  )
)
ORDER BY LAST_LOAD_TIME DESC;

--did task trigger or fail for post ingestion?
SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'PROCESS_CLIENT_DATA_UPLOAD_TASK',
    RESULT_LIMIT => 50
  )
)
ORDER BY SCHEDULED_TIME DESC;
--Uncaught exception of type 'STATEMENT_ERROR' on line 89 at position 4 : DML operation to table CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS failed on column APP_BATCH_ID with error: Numeric value '60b2ddf1-7d43-4313-b98a-f90685478fc7' is not recognized
--Uncaught exception of type 'STATEMENT_ERROR' on line 36 at position 4 : SQL compilation error: error line 16 at position 11
--invalid identifier 'B.APP_BATCH_ID'
--Check task proc

--is it in raw? 
SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED;
--uploads/client_1d96f728-bdc7-4554-8167-89cbd072b075/batch_8ba86a47-2574-4aab-8ecb-89026eb8e72c/2e98271d-b842-4952-999f-2ec5362c88ce_10if_test_updated_sample.csv
--uploads/client_1d96f728-bdc7-4554-8167-89cbd072b075/batch_2fad89d6-4479-4cb4-848c-cb6786399394/e161db21-1455-44ba-a27f-14dc83424d18_13if_test_updated_sample.csv
--uploads/client_1d96f728-bdc7-4554-8167-89cbd072b075/batch_60b2ddf1-7d43-4313-b98a-f90685478fc7/db881f2c-706f-4bcb-9747-e1b502fb5c35_14if_test_updated_sample.csv
--Check next step incase it was cleared

--Does file name exist?
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;
--batch id, client id, appfile id
--1502	201, e09a8117-13d8-4526-b656-b4b429404d81
-- prcessed ts set
-- Yes but delivery completed null

-- Did enrichment status get inserted?
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS;
--No - problem with process client to raw, check if data made it to the raw input

--data in raw input
SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT
WHERE BATCH_ID = 1801;
--yes

SELECT *
FROM CUSTOMER.MANAGEMENT.CLIENT
WHERE ID = 201;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION;

SE:E


SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED;

USE SCHEMA CUSTOMER.RAW;
CREATE OR REPLACE TEMP TABLE TMP_PERSON_INPUT_STREAM AS
    SELECT
        S.STORAGE_FILE_NAME,
        S.RECORD_ID,
        S.FIRST_NAME,
        S.LAST_NAME,
        S.ZIP,
        S.LOAD_TS,

        /* Parse ORG_ID from ...client_<ORG_ID>/... */
        REGEXP_SUBSTR(S.STORAGE_FILE_NAME, 'client_([^/]+)', 1, 1, 'e', 1) AS APP_ORGANIZATION_ID,

        /* Parse BATCH_ID from .../batch_<BATCH_ID>/... */
        REGEXP_SUBSTR(S.STORAGE_FILE_NAME, 'batch_([^/]+)', 1, 1, 'e', 1) AS APP_BATCH_ID,

        /* Last path segment, e.g. abc123_myfile.csv */
        REGEXP_SUBSTR(S.STORAGE_FILE_NAME, '[^/]+$') AS FILE_BASENAME,

        /* Remove UUID_ prefix from basename to get original source file name */
        REGEXP_REPLACE(
            REGEXP_SUBSTR(S.STORAGE_FILE_NAME, '[^/]+$'),
            '^[^_]+_',
            ''
        ) AS ORIGINAL_SOURCE_FILE_NAME
    FROM CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED S;

    -- DELETE
    -- FROM CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED;

    SELECT *
    FROM TMP_PERSON_INPUT_STREAM;

 -- INSERT INTO CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS (
 --        CLIENT_ID,
 --        BATCH_ID,
 --        APP_BATCH_ID,
 --        ENRICHMENT_TYPE,
 --        ENRICHMENT_TYPE_CODE,
 --        ENRICHMENT_STATUS
 --    )
    SELECT DISTINCT
        B.CLIENT_ID,
        B.ID AS BATCH_ID,
        T.APP_BATCH_ID AS APP_BATCH_ID,
        A.VALUE:label::VARCHAR AS ENRICHMENT_TYPE,
        A.VALUE:code::VARCHAR AS ENRICHMENT_TYPE_CODE,
        'PENDING' AS ENRICHMENT_STATUS
    FROM (
        SELECT DISTINCT
            APP_ORGANIZATION_ID,
            APP_BATCH_ID
        FROM TMP_PERSON_INPUT_STREAM
    ) T
    INNER JOIN CUSTOMER.MANAGEMENT.CLIENT C
        ON C.APP_ORGANIZATION_ID = T.APP_ORGANIZATION_ID
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH B
        ON B.CLIENT_ID = C.ID
       AND B.APP_BATCH_ID =T.APP_BATCH_ID
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION CFG
        ON CFG.APP_ORGANIZATION_ID = T.APP_ORGANIZATION_ID
       AND CFG.APP_BATCH_ID = T.APP_BATCH_ID
    , LATERAL FLATTEN(INPUT => CFG.CONFIG_JSON:attributes) A
    WHERE A.VALUE:code IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS S
          WHERE S.CLIENT_ID = B.CLIENT_ID
            AND S.BATCH_ID = B.ID
            AND S.APP_BATCH_ID = T.APP_BATCH_ID
            AND S.ENRICHMENT_TYPE_CODE = A.VALUE:code::VARCHAR
      );


      SELECT
    CFG.APP_ORGANIZATION_ID,
    CFG.APP_BATCH_ID,
    CFG.CONFIG_JSON
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION CFG
WHERE CFG.APP_ORGANIZATION_ID = '1d96f728-bdc7-4554-8167-89cbd072b075'
  AND CFG.APP_BATCH_ID = 'e09a8117-13d8-4526-b656-b4b429404d81';

  SELECT
    A.VALUE:code::VARCHAR AS CODE,
    A.VALUE:label::VARCHAR AS LABEL
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION CFG,
LATERAL FLATTEN(INPUT => CFG.CONFIG_JSON:attributes) A
WHERE CFG.APP_ORGANIZATION_ID = '1d96f728-bdc7-4554-8167-89cbd072b075'
  AND CFG.APP_BATCH_ID = 'e09a8117-13d8-4526-b656-b4b429404d81';



--Does presets exist?
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION;