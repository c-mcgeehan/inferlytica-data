
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
SELECT SYSTEM$PIPE_STATUS('CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE');
DESC PIPE CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE;
--{"executionState":"RUNNING","pendingFileCount":0,"lastIngestedTimestamp":"2026-04-02T10:56:02.753Z","lastIngestedFilePath":"client_0922cdb9-f46e-4b15-a511-a902d2c913b2/batch_92fa1dff-56e9-4575-8ab0-18e63d4aa0dc/5c7303c0-02d7-42f6-9447-2b540aa8086b_444updated_sample.csv","notificationChannelName":"arn:aws:sqs:us-east-1:004878718171:sf-snowpipe-AIDAQCIWLKTNUANPOWPIV-Ym7zU2p6AX2KU7RyzdJdDg","numOutstandingMessagesOnChannel":1,"lastReceivedMessageTimestamp":"2026-04-02T10:56:02.591Z","lastForwardedMessageTimestamp":"2026-04-02T10:56:02.766Z","lastPulledFromChannelTimestamp":"2026-04-02T10:59:32.581Z","lastForwardedFilePath":"inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/client_0922cdb9-f46e-4b15-a511-a902d2c913b2/batch_92fa1dff-56e9-4575-8ab0-18e63d4aa0dc/5c7303c0-02d7-42f6-9447-2b540aa8086b_444updated_sample.csv"}


SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED',
    START_TIME => DATEADD('hour', -2, CURRENT_TIMESTAMP()),
    PIPE_NAME => 'CUSTOMER.FILE_PROCESSING.CLIENT_DATA_PIPE'
  )
)
ORDER BY LAST_LOAD_TIME DESC;

--"lastReceivedMessageTimestamp":   2026-04-02 03:56:02.591 (receipt time of new file from AWS S3 Event)
--process data loaded:              2026-04-02 03:56:02.885 (pipe has landed all data into raw.preprocessed input table)
--client batch data loaded to raw:  2026-04-02 03:56:24.854 (CREATED_TS - when batch record inserts/created from task)
--client batch processed:           2026-04-02 03:56:26.682 (after data is landed in raw - finished)

--Analyzing the above:
--After we receive our notification pipe takes less than half a second to move 100 records into preprocess table
--After preprocess data landed, stream takes ~22 seconds to trigger task & create a batch from the data.SELECT
--After batch is created it takes approximately ~2 seconds to load the data to raw and mark batch as processed (one delete happens after this to clear our preprocess)
-- all in all: 30 seconds or less from file drop to data available in raw for 100 records
-- next steps: 
    -- refresh dynamic tables to get results avaialable (Data avaialable for modeling ts?)
    -- execute delivery procedure for gender / age buckets predictions (data delivered ts?)


SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;


SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT
WHERE BATCH_ID = 301;