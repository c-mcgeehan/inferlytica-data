
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
WHERE BATCH_ID = 1503;
--yes

SELECT *
FROM CUSTOMER.MANAGEMENT.CLIENT
WHERE ID = 201;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION;

SELECT *
FROM CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE
WHERE BATCH_ID = 1503;

  SELECT DISTINCT
        b.CLIENT_ID,
        b.BATCH_ID
 FROM CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE b
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH cb
        ON b.CLIENT_ID = cb.CLIENT_ID
       AND b.BATCH_ID = cb.ID
    WHERE cb.DELIVERY_COMPLETE_TS IS NULL;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.OUTPUT_GENDER_RESULTS
WHERE BATCH_ID = 1503;


SELECT *
FROM CUSTOMER.FILE_PROCESSING.GENDER_RESULTS_BATCH_SUMMARY;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS;

SELECT
FROM DATA_PROVIDER_SSA.RAW.FIRST_NAME_GENDER_QUANTITY_YEAR;

{"analysis_type":"gender_metrics","metrics":[{"metric_name":"Gender","metric_type":"COUNT","metrics":[{"label":"F","value":48},{"label":"M","value":50},{"label":"U","value":2}]},{"metric_name":"Confidence Level","metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":1},{"label":"HIGH","value":97},{"label":"LOW","value":1},{"label":"MEDIUM","value":1}]},{"metric_name":"Uncommon Names","metric_type":"COUNT","metrics":[{"label":"TOTAL_UNCOMMON_NAMES","value":0}]},{"metric_name":"Missing First Name","metric_type":"COUNT","metrics":[{"label":"MISSING_FIRST_NAME","value":0}]},{"metric_name":"Average Max Probability","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_MAX_PROBABILITY","value":0.981638}]},{"metric_name":"Average Probability Gap","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_PROBABILITY_GAP","value":0.963277}]}]}