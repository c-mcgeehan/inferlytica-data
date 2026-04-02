create or replace transient table CUSTOMER.RAW.PERSON_INPUT_PREPROCESSED (
    RECORD_ID varchar NOT NULL,
    FIRST_NAME varchar,
    LAST_NAME varchar,
    ZIP varchar,
    STORAGE_FILE_NAME VARCHAR,
    FILE_RECORD_ROW_NUMBER NUMBER(32,0),
    LOAD_TS timestamp_ntz
);
