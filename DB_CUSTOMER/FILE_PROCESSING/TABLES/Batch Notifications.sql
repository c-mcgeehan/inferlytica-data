CREATE OR REPLACE TRANSIENT TABLE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
(
    ID NUMBER(32,0) IDENTITY(1,1),
    CLIENT_ID NUMBER(32,0) NOT NULL,
    BATCH_ID NUMBER(32, 0) NOT NULL,
    APP_FILE_ID VARCHAR NOT NULL,
    APP_ORGANIZATION_ID VARCHAR NOT NULL,
    PAYLOAD VARCHAR,
    EVENT_TYPE VARCHAR,
    STATUS VARCHAR,
    ATTEMPT_COUNT NUMBER(4, 0) DEFAULT 0,
    LAST_ATTEMPT_TS timestamp_ntz,
    LAST_ERROR VARCHAR,
    CREATED_TS timestamp_ntz default current_timestamp(),
    PROCESSED_TS timestamp_ntz

);

ALTER TABLE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
ADD  COLUMN APP_BATCH_ID VARCHAR 

ALTER TABLE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
RENAME COLUMN APP_FILE_ID TO APP_BATCH_ID;


    CONSTRAINT foreign_key_batch_id
        FOREIGN KEY (BATCH_ID)
        REFERENCES CUSTOMER.FILE_PROCESSING.CLIENT_BATCH(ID)


        
     CONSTRAINT foreign_key_client_id
        FOREIGN KEY (CLIENT_ID)
        REFERENCES CUSTOMER.MANAGEMENT.CLIENT(ID);


SELECT *
FROM  CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS;

{"batch_id":"191bf635-1bec-4a08-994d-a98a9e9c1b0b","delivery_status":"READY","delivery_status_updated_at":"2026-04-06 10:22:35.917 -0700","file_analysis":[{"analysis_type":"gender_metrics","metrics":[{"metric_name":"Average Max Probability","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_MAX_PROBABILITY","value":0.981638}]},{"metric_name":"Average Probability Gap","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_PROBABILITY_GAP","value":0.963277}]},{"metric_name":"Confidence Level","metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":1},{"label":"HIGH","value":97},{"label":"LOW","value":1},{"label":"MEDIUM","value":1}]},{"metric_name":"Gender","metric_type":"COUNT","metrics":[{"label":"F","value":48},{"label":"M","value":50},{"label":"U","value":2}]},{"metric_name":"Missing First Name","metric_type":"COUNT","metrics":[{"label":"MISSING_FIRST_NAME","value":0}]},{"metric_name":"Total Records","metric_type":"COUNT","metrics":[{"label":"TOTAL_RECORDS","value":100}]},{"metric_name":"Uncommon Name Confidence Level","metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":0},{"label":"HIGH","value":0},{"label":"LOW","value":0},{"label":"MEDIUM","value":0}]},{"metric_name":"Uncommon Names","metric_type":"COUNT","metrics":[{"label":"TOTAL_UNCOMMON_NAMES","value":0}]}]}],"organization_id":"1d96f728-bdc7-4554-8167-89cbd072b075"}