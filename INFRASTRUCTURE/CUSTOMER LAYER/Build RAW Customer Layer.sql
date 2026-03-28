CREATE DATABASE CUSTOMER;
CREATE SCHEMA CUSTOMER.RAW;


create or replace transient table CUSTOMER.RAW.PERSON_INPUT (
    CLIENT_ID NUMBER(32,0),
    BATCH_ID NUMBER(32, 0),
    RECORD_ID varchar NOT NULL,
    FIRST_NAME varchar,
    LAST_NAME varchar,
    ZIP varchar,
    LOAD_TS timestamp_ntz default current_timestamp()
);


UPDATE CUSTOMER.RAW.PERSON_INPUT
SET RECORD_ID = UUID_STRING();