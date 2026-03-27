CREATE DATABASE CUSTOMER;
CREATE SCHEMA CUSTOMER.RAW;


create or replace transient table CUSTOMER.RAW.PERSON_INPUT (
    CUSTOMER_ID NUMBER(32,0),
    RECORD_ID varchar,
    FIRST_NAME varchar,
    LAST_NAME varchar,
    ZIP varchar,
    LOAD_TS timestamp_ntz default current_timestamp()
);
