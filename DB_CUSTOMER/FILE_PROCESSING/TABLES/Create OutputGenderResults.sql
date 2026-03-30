create or replace transient table CUSTOMER.FILE_PROCESSING.OUTPUT_GENDER_RESULTS (
    CLIENT_ID NUMBER(32,0),
    BATCH_ID NUMBER(32, 0),
    RECORD_ID varchar,
    PREDICTED_GENDER varchar,
    REPORTABLE_GENDER varchar,
    CONFIDENCE_LEVEL varchar,
    MAX_PROBABILITY number(10,6),
    PROBABILITY_GAP number(10,6),
    MODEL_VERSION varchar,
    PROCESSED_TS timestamp_ntz
);