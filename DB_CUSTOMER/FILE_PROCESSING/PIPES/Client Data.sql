
CREATE OR REPLACE PIPE CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED
  FROM 
  (
     SELECT
        t.$1::VARCHAR AS RECORD_ID,
        t.$2::VARCHAR AS FIRST_NAME,
        t.$3::VARCHAR AS LAST_NAME,
        t.$4::VARCHAR AS ZIP,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    METADATA$START_SCAN_TIME
    FROM @CUSTOMER.FILE_PROCESSING.CLIENT_DATA_STAGE AS t
  )
  FILE_FORMAT = CUSTOMER.FILE_PROCESSING.CUSTOMER_RAW_FORMAT;

  --arn:aws:sqs:us-east-1:004878718171:sf-snowpipe-AIDAQCIWLKTNUANPOWPIV-Ym7zU2p6AX2KU7RyzdJdDg

DESC PIPE CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE;
--{"executionState":"RUNNING","pendingFileCount":0,"lastIngestedTimestamp":"2026-04-02T10:56:02.753Z","lastIngestedFilePath":"client_0922cdb9-f46e-4b15-a511-a902d2c913b2/batch_92fa1dff-56e9-4575-8ab0-18e63d4aa0dc/5c7303c0-02d7-42f6-9447-2b540aa8086b_444updated_sample.csv","notificationChannelName":"arn:aws:sqs:us-east-1:004878718171:sf-snowpipe-AIDAQCIWLKTNUANPOWPIV-Ym7zU2p6AX2KU7RyzdJdDg","numOutstandingMessagesOnChannel":1,"lastReceivedMessageTimestamp":"2026-04-02T10:56:02.591Z","lastForwardedMessageTimestamp":"2026-04-02T10:56:02.766Z","lastPulledFromChannelTimestamp":"2026-04-02T10:59:32.581Z","lastForwardedFilePath":"inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/client_0922cdb9-f46e-4b15-a511-a902d2c913b2/batch_92fa1dff-56e9-4575-8ab0-18e63d4aa0dc/5c7303c0-02d7-42f6-9447-2b540aa8086b_444updated_sample.csv"}


--1: 
SELECT SYSTEM$PIPE_STATUS('CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE');
{"executionState":"RUNNING","pendingFileCount":0,"lastIngestedTimestamp":"2026-04-02T11:47:19.581Z","lastIngestedFilePath":"client_0922cdb9-f46e-4b15-a511-a902d2c913b2/batch_41c0d8cd-6be4-432f-9689-19f0dae177a2/e02505e5-2db5-4ec3-a602-9fa8d46fb6df_555updated_sample.csv","notificationChannelName":"arn:aws:sqs:us-east-1:004878718171:sf-snowpipe-AIDAQCIWLKTNUANPOWPIV-Ym7zU2p6AX2KU7RyzdJdDg","numOutstandingMessagesOnChannel":1,"lastReceivedMessageTimestamp":"2026-04-02T11:47:19.166Z","lastForwardedMessageTimestamp":"2026-04-02T11:47:19.938Z","lastPulledFromChannelTimestamp":"2026-04-02T11:49:29.077Z","lastForwardedFilePath":"inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/client_0922cdb9-f46e-4b15-a511-a902d2c913b2/batch_41c0d8cd-6be4-432f-9689-19f0dae177a2/e02505e5-2db5-4ec3-a602-9fa8d46fb6df_555updated_sample.csv"}
--1:
SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED',
    START_TIME => DATEADD('hour', -2, CURRENT_TIMESTAMP()),
    PIPE_NAME => 'CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE'
  )
)
ORDER BY LAST_LOAD_TIME DESC;

--2 (the time load_ts comes from preprocess pipe scan time):
SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT;

SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED;

--3 & 4:
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;

--1: "lastReceivedMessageTimestamp":   2026-04-02 03:56:02.591 (receipt time of new file from AWS S3 Event)
--2: process data loaded:              2026-04-02 03:56:02.885 (pipe has landed all data into raw.preprocessed input table)
--3: client batch data loaded to raw:  2026-04-02 03:56:24.854 (CREATED_TS - when batch record inserts/created from task)
--4; client batch processed:           2026-04-02 03:56:26.682 (after data is landed in raw - finished)

--Analyzing the above:
--After we receive our notification pipe takes less than half a second to move 100 records into preprocess table
--After preprocess data landed, stream takes ~22 seconds to trigger task & create a batch from the data.SELECT
--After batch is created it takes approximately ~2 seconds to load the data to raw and mark batch as processed (one delete happens after this to clear our preprocess)
-- all in all: 30 seconds or less from file drop to data available in raw for 100 records
-- next steps: 
    -- refresh dynamic tables to get results avaialable (Data avaialable for modeling ts?)
    -- execute delivery procedure for gender / age buckets predictions (data delivered ts?)

--After serverless task changes (28 seconds) - actually worse:

--1: 2026-04-02 04:47:19.166
--2  2026-04-02 04:47:25.245
--3: 2026-04-02 04:47:45.609
--4: 2026-04-02 04:47:47.890



