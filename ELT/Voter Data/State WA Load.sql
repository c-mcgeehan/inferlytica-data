--From local machine using Snowflake CLI - Powershell


-- Step 1: snow connection test
-- Step 2: snow sql
-- Step 3: USE DATABASE DATA_PROVIDER_VOTER;

-- If Stage & File Format is not yet created
-- Step 3a: CREATE OR REPLACE FILE FORMAT VOTER_WA_REGISTRATION_CSV TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=1 COMMENT='For Washington State Voter Registration File';
-- Step 3b: CREATE STAGE VOTER_WA_REGISTRATION FILE_FORMAT = VOTER_WA_REGISTRATION_CSV;

-- Step 4: Make sure files aren't currently in Stage: LIST @VOTER_WA_REGISTRATION;
-- Step 4a: If files are in Stage: REMOVE @VOTER_WA_REGISTRATION;

-- Step 5: Using actual local path: PUT file://C:\Users\Charles\Documents\Inferlytica\datasets\voter\washington\03.2026.wa\03.2026.WA\20260301_VRDB_Extract.txt @VOTER_WA_REGISTRATION;
-- Step 6: SELECT INTO OR COPY INTO DATA_PROVIDER_VOTER.RAW.STATE_WA_VOTER_REGISTRATION FROM @VOTER_WA_REGISTRATION;

USE DATABASE DATA_PROVIDER_VOTER;
LIST @VOTER_WA_REGISTRATION;
REMOVE @VOTER_WA_REGISTRATION;

CREATE OR REPLACE FILE FORMAT VOTER_WA_REGISTRATION_CSV TYPE=CSV FIELD_DELIMITER='|' SKIP_HEADER=1 COMMENT='For Washington State Voter Registration File' ENCODING = 'ISO-8859-1';
;
CREATE OR REPLACE STAGE VOTER_WA_REGISTRATION FILE_FORMAT = VOTER_WA_REGISTRATION_CSV;

INSERT INTO DATA_PROVIDER_VOTER.RAW.STATE_WA_VOTER_REGISTRATION (
    STATE_VOTER_ID,
    FIRST_NAME,
    MIDDLE_NAME,
    LAST_NAME,
    NAME_SUFFIX,
    BIRTH_YEAR,
    GENDER,
    REGISTRATION_STREET_NUMBER,
    REGISTRATION_STREET_FRACTION,
    REGISTRATION_STREET_NAME,
    REGISTRATION_STREET_TYPE,
    REGISTRATION_UNIT_TYPE,
    REGISTRATION_STREET_PRE_DIRECTION,
    REGISTRATION_STREET_POST_DIRECTION,
    REGISTRATION_UNIT_NUMBER,
    REGISTRATION_CITY,
    REGISTRATION_STATE,
    REGISTRATION_ZIP_CODE,
    COUNTY_CODE,
    PRECINCT_CODE,
    PRECINCT_PART,
    LEGISLATIVE_DISTRICT,
    CONGRESSIONAL_DISTRICT,
    MAILING_ADDRESS_LINE_1,
    MAILING_ADDRESS_LINE_2,
    MAILING_ADDRESS_LINE_3,
    MAILING_CITY,
    MAILING_ZIP_CODE,
    MAILING_STATE,
    MAILING_COUNTRY,
    REGISTRATION_DATE,
    LAST_VOTED_DATE,
    STATUS_CODE
)
SELECT
    TRY_TO_NUMBER(t.$1)       AS STATE_VOTER_ID,
    t.$2                      AS FIRST_NAME,
    t.$3                      AS MIDDLE_NAME,
    t.$4                      AS LAST_NAME,
    t.$5                      AS NAME_SUFFIX,
    TRY_TO_NUMBER(t.$6)       AS BIRTH_YEAR,
    t.$7                      AS GENDER,
    t.$8                      AS REGISTRATION_STREET_NUMBER,
    t.$9                      AS REGISTRATION_STREET_FRACTION,
    t.$10                     AS REGISTRATION_STREET_NAME,
    t.$11                     AS REGISTRATION_STREET_TYPE,
    t.$12                     AS REGISTRATION_UNIT_TYPE,
    t.$13                     AS REGISTRATION_STREET_PRE_DIRECTION,
    t.$14                     AS REGISTRATION_STREET_POST_DIRECTION,
    t.$15                     AS REGISTRATION_UNIT_NUMBER,
    t.$16                     AS REGISTRATION_CITY,
    t.$17                     AS REGISTRATION_STATE,
    t.$18                     AS REGISTRATION_ZIP_CODE,
    t.$19                     AS COUNTY_CODE,
    t.$20                     AS PRECINCT_CODE,
    t.$21                     AS PRECINCT_PART,
    t.$22                     AS LEGISLATIVE_DISTRICT,
    t.$23                     AS CONGRESSIONAL_DISTRICT,
    t.$24                     AS MAILING_ADDRESS_LINE_1,
    t.$25                     AS MAILING_ADDRESS_LINE_2,
    t.$26                     AS MAILING_ADDRESS_LINE_3,
    t.$27                     AS MAILING_CITY,
    t.$28                     AS MAILING_ZIP_CODE,
    t.$29                     AS MAILING_STATE,
    t.$30                     AS MAILING_COUNTRY,
    TRY_TO_DATE(t.$31)        AS REGISTRATION_DATE,
    TRY_TO_DATE(t.$32)        AS LAST_VOTED_DATE,
    t.$33                     AS STATUS_CODE
FROM @VOTER_WA_REGISTRATION (pattern=>'20260301_VRDB_Extract.txt.gz') t;

DELETE
FROM DATA_PROVIDER_VOTER.RAW.STATE_WA_VOTER_REGISTRATION;




-- Step 7: Verify Row count appears accurate based on results in Step 6 (empty rows are skipped I think)
-- Step 8: Clean up Stage: REMOVE @VOTER_WA_REGISTRATION;