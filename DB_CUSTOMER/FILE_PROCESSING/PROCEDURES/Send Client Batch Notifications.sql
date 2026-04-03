CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.SEND_CLIENT_BATCH_NOTIFICATIONS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN

    --PREP stream data
    CREATE OR REPLACE TEMP TABLE TMP_CLIENT_BATCH_NOTIFICATIONS AS
    SELECT
        ID,
        CLIENT_ID,
        BATCH_ID,
        APP_FILE_ID,
        APP_ORGANIZATION_ID,
        PAYLOAD,
        EVENT_TYPE,
        STATUS,
        ATTEMPT_COUNT,
        LAST_ATTEMPT_TS,
        LAST_ERROR,
    FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS_STREAM S;

    -- Send notification to supabase per record
    -- Updates notification (CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS) records on table to be marked as failed, succeeded etc...
    -- MAKE THIS A PYTHON PROC that loops through and makes the api requests then gets a response and handles one at a time
    
    RETURN 'Processed ' || (SELECT COUNT(*) FROM TMP_CLIENT_BATCH_NOTIFICATIONS) || ' stream rows.';
END;
$$;