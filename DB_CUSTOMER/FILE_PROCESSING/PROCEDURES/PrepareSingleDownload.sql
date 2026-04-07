CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.PREPARE_SINGLE_DOWNLOAD(
    APP_ORGANIZATION_ID VARCHAR,
    APP_BATCH_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    V_CLIENT_ID NUMBER(32,0);
    V_BATCH_ID NUMBER(32,0);
    V_OBJECT_KEY VARCHAR;
    V_COPY_SQL VARCHAR;
    V_NOW TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
BEGIN

    SELECT
        C.ID,
        B.ID
    INTO :V_CLIENT_ID, :V_BATCH_ID
    FROM CUSTOMER.MANAGEMENT.CLIENT C
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH B
        ON B.CLIENT_ID = C.ID
    WHERE C.APP_ORGANIZATION_ID = :APP_ORGANIZATION_ID
      AND B.APP_BATCH_ID = :APP_BATCH_ID;

    IF (V_BATCH_ID IS NULL) THEN
        RETURN 'No batch found for APP_ORGANIZATION_ID=' || APP_ORGANIZATION_ID
            || ', APP_BATCH_ID=' || APP_BATCH_ID;
    END IF;

    /* Deterministic S3 object key */
    V_OBJECT_KEY := 'downloads/'
        || APP_ORGANIZATION_ID || '/'
        || APP_BATCH_ID || '/'
        || 'results_' || APP_BATCH_ID || '.csv';

    /*
      Replace the stage name and SELECT source below.
      The SELECT should return the final file contents for this batch.
    */
    V_COPY_SQL := '
        COPY INTO @CUSTOMER.FILE_PROCESSING.DOWNLOAD_EXPORT_STAGE/' || V_OBJECT_KEY || '
        FROM (
            SELECT *
            FROM CUSTOMER.FILE_PROCESSING.OUTPUT_FINAL_RESULTS
            WHERE CLIENT_ID = ' || V_CLIENT_ID || '
              AND BATCH_ID = ' || V_BATCH_ID || '
        )
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
            COMPRESSION = NONE
            NULL_IF = ('''')
        )
        HEADER = TRUE
        SINGLE = TRUE
        OVERWRITE = TRUE
        MAX_FILE_SIZE = 4900000000
    ';

    EXECUTE IMMEDIATE :V_COPY_SQL;

    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH
    SET
        DOWNLOAD_OBJECT_KEY = :V_OBJECT_KEY
    WHERE CLIENT_ID = :V_CLIENT_ID
      AND ID = :V_BATCH_ID;

    RETURN 'Prepared download for APP_BATCH_ID=' || APP_BATCH_ID
        || ', object key=' || V_OBJECT_KEY;

EXCEPTION
    WHEN STATEMENT_ERROR THEN
        RETURN 'Failed to prepare download for APP_BATCH_ID=' || APP_BATCH_ID
            || ': ' || SQLERRM;
END;
$$;

If you want, I can n