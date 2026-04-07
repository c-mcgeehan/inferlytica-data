
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
--Uncaught exception of type 'STATEMENT_ERROR' on line 36 at position 4 : SQL compilation error: error line 16 at position 11
--invalid identifier 'B.APP_BATCH_ID'
--Check task proc

--is it in raw? 
SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED;
--uploads/client_1d96f728-bdc7-4554-8167-89cbd072b075/batch_8ba86a47-2574-4aab-8ecb-89026eb8e72c/2e98271d-b842-4952-999f-2ec5362c88ce_10if_test_updated_sample.csv
--Check next step incase it was cleared

--Does file name exist?
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;
--batch id, client id, appfile id
--1601	201, 7bca9c92-b96e-445a-9b5d-39e90af4c27a
-- Yes but delivery completed null

-- Did enrichment status get inserted?
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS;
--No - problem with process client to raw, check if data made it to the raw input

--data in raw input
SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT
WHERE BATCH_ID = 1601;
--yes

SELECT *
FROM CUSTOMER.MANAGEMENT.CLIENT
WHERE ID = 201;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION;

 SELECT DISTINCT
        B.CLIENT_ID,
        B.ID AS BATCH_ID,
        TRY_TO_NUMBER(B.APP_FILE_ID) AS APP_FILE_ID,
        A.VALUE:label::VARCHAR AS ENRICHMENT_TYPE,
        A.VALUE:code::VARCHAR AS ENRICHMENT_TYPE_CODE,
        'PENDING' AS ENRICHMENT_STATUS
    FROM  CUSTOMER.MANAGEMENT.CLIENT C
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH B
        ON B.CLIENT_ID = C.ID
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION CFG
        ON CFG.APP_ORGANIZATION_ID = C.APP_ORGANIZATION_ID
       AND CFG.APP_FILE_ID = B.APP_FILE_ID
    , LATERAL FLATTEN(INPUT => CFG.CONFIG_JSON:attributes) A
    WHERE b.ID=  1501
    AND
    A.VALUE:code IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS S
          WHERE S.CLIENT_ID = B.CLIENT_ID
            AND S.BATCH_ID = B.ID
            AND S.APP_FILE_ID = TRY_TO_NUMBER(B.APP_FILE_ID)
            AND S.ENRICHMENT_TYPE_CODE = A.VALUE:code::VARCHAR
      );



--Does presets exist?
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION;