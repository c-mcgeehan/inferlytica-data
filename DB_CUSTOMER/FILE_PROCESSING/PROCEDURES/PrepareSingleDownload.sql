--find a batch to deliver
--find where we have raw name data STRIP_NULL_VALUE(
--join gender DATABASE
--Prepared download for APP_BATCH_ID=550240cf-f4b4-49e0-a51c-49cade26eb1f, object key=downloads/1d96f728-bdc7-4554-8167-89cbd072b075/550240cf-f4b4-49e0-a51c-49cade26eb1f/results_550240cf-f4b4-49e0-a51c-49cade26eb1f.csv
SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE;

 SELECT
        C.ID,
        B.ID
    FROM CUSTOMER.MANAGEMENT.CLIENT C
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH B
        ON B.CLIENT_ID = C.ID
        WHERE C.APP_ORGANIZATION_ID = '1d96f728-bdc7-4554-8167-89cbd072b075'
      AND B.APP_BATCH_ID = '550240cf-f4b4-49e0-a51c-49cade26eb1f';
      
 SELECT LISTAGG(
           'SRC.' || F.VALUE:field_name::VARCHAR,
           ', '
       ) WITHIN GROUP (
           ORDER BY
               IFF(F.VALUE:sort_order IS NULL, 0, 1),
               F.VALUE:sort_order::NUMBER,
               F.VALUE:field_name::VARCHAR
       )
    FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION C,
    LATERAL FLATTEN(INPUT => C.CONFIG_JSON:output_fields) F
    WHERE C.APP_ORGANIZATION_ID = '1d96f728-bdc7-4554-8167-89cbd072b075'
      AND C.APP_BATCH_ID = '550240cf-f4b4-49e0-a51c-49cade26eb1f'
      AND F.VALUE:field_name IS NOT NULL;

SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT;
    
                SELECT
                    /* put all possible exportable columns here */
                    R.RECORD_ID,
                    R.FIRST_NAME,
                    R.LAST_NAME,
                    R.ZIP,
                    G.MALE_PROBABILITY,
                    G.FEMALE_PROBABILITY,
                    G.CONFIDENCE_LEVEL AS GENDER_CONFIDENCE_LEVEL,
                    G.REPORTABLE_GENDER AS PREDICTED_GENDER
                FROM CUSTOMER.RAW.PERSON_INPUT R
                LEFT JOIN CUSTOMER.FILE_PROCESSING.OUTPUT_GENDER_RESULTS G
                  ON R.CLIENT_ID = G.CLIENT_ID
                 AND R.BATCH_ID = G.BATCH_ID
                 AND R.RECORD_ID = G.RECORD_ID
                WHERE R.CLIENT_ID = 201
                  AND R.BATCH_ID = 2001;

CALL CUSTOMER.FILE_PROCESSING.PREPARE_SINGLE_DOWNLOAD('1d96f728-bdc7-4554-8167-89cbd072b075'::VARCHAR, 'f910dc1a-cfd9-4886-9287-506521d95d77'::VARCHAR);

CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.PREPARE_SINGLE_DOWNLOAD(
    APP_ORGANIZATION_ID VARCHAR,
    APP_BATCH_ID VARCHAR
) COPY GRANTS
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    V_CLIENT_ID NUMBER(32,0);
    V_BATCH_ID NUMBER(32,0);
    V_OBJECT_PATH VARCHAR;
    V_OBJECT_KEY VARCHAR;
    V_COPY_SQL VARCHAR;
    V_SELECT_LIST VARCHAR;
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
    V_OBJECT_PATH := APP_ORGANIZATION_ID || '/'
        || APP_BATCH_ID || '/'
        || 'results_' || APP_BATCH_ID || '.csv.gz';

    V_OBJECT_KEY := 'downloads/' || V_OBJECT_PATH;

    SELECT LISTAGG(
           'SRC.' || F.VALUE:field_name::VARCHAR,
           ', '
       ) WITHIN GROUP (
           ORDER BY
               IFF(F.VALUE:sort_order IS NULL, 0, 1),
               F.VALUE:sort_order::NUMBER,
               F.VALUE:field_name::VARCHAR
       )
    INTO :V_SELECT_LIST
    FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION C,
    LATERAL FLATTEN(INPUT => C.CONFIG_JSON:output_fields) F
    WHERE C.APP_ORGANIZATION_ID = :APP_ORGANIZATION_ID
      AND C.APP_BATCH_ID = :APP_BATCH_ID
      AND F.VALUE:field_name IS NOT NULL;

    /*
      Replace the stage name and SELECT source below.
      The SELECT should return the final file contents for this batch.
    */
    V_COPY_SQL := '
        COPY INTO @CUSTOMER.FILE_PROCESSING.CLIENT_DATA_DOWNLOAD_STAGE/' || V_OBJECT_PATH || '
        FROM (
            SELECT ' || V_SELECT_LIST || '
            FROM (
                SELECT
                    /* put all possible exportable columns here */
                    R.RECORD_ID,
                    R.FIRST_NAME,
                    R.LAST_NAME,
                    R.ZIP,
                    G.MALE_PROBABILITY,
                    G.FEMALE_PROBABILITY,
                    G.CONFIDENCE_LEVEL AS GENDER_CONFIDENCE_LEVEL,
                    G.REPORTABLE_GENDER AS PREDICTED_GENDER
                FROM CUSTOMER.RAW.PERSON_INPUT R
                LEFT JOIN CUSTOMER.FILE_PROCESSING.OUTPUT_GENDER_RESULTS G
                  ON R.CLIENT_ID = G.CLIENT_ID
                 AND R.BATCH_ID = G.BATCH_ID
                 AND R.RECORD_ID = G.RECORD_ID
                WHERE R.CLIENT_ID = ' || V_CLIENT_ID || '
                  AND R.BATCH_ID = ' || V_BATCH_ID || '
            ) SRC
        )
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
            --COMPRESSION = NONE
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

    MERGE INTO CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE tgt
    USING (
        SELECT
            :APP_ORGANIZATION_ID AS APP_ORGANIZATION_ID,
            :APP_BATCH_ID AS APP_BATCH_ID,
            :V_OBJECT_KEY AS DOWNLOAD_OBJECT_KEY,
            'READY' AS DOWNLOAD_STATUS,
            CURRENT_TIMESTAMP() AS TS
    ) src
    ON tgt.APP_ORGANIZATION_ID = src.APP_ORGANIZATION_ID
    AND tgt.APP_BATCH_ID = src.APP_BATCH_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.DOWNLOAD_OBJECT_KEY = src.DOWNLOAD_OBJECT_KEY,
        tgt.DOWNLOAD_STATUS = src.DOWNLOAD_STATUS,
        tgt.PROCESSING_STATUS = 'PENDING',
        tgt.PROCESSING_STARTED_TS = NULL,
        tgt.PROCESSED_TS = NULL,
        tgt.ERROR_MESSAGE = NULL,
        tgt.UPDATED_TS = src.TS
    WHEN NOT MATCHED THEN INSERT (
        APP_ORGANIZATION_ID,
        APP_BATCH_ID,
        DOWNLOAD_OBJECT_KEY,
        DOWNLOAD_STATUS,
        QUEUED_TS,
        PROCESSING_STATUS,
        CREATED_TS,
        UPDATED_TS
    )
    VALUES (
        src.APP_ORGANIZATION_ID,
        src.APP_BATCH_ID,
        src.DOWNLOAD_OBJECT_KEY,
        src.DOWNLOAD_STATUS,
        src.TS,
        'PENDING',
        src.TS,
        src.TS
    );

    RETURN 'Prepared download for APP_BATCH_ID=' || APP_BATCH_ID
        || ', object key=' || V_OBJECT_KEY;

EXCEPTION
    WHEN STATEMENT_ERROR THEN
        RETURN 'Failed to prepare download for APP_BATCH_ID=' || APP_BATCH_ID
            || ': ' || SQLERRM;
END;
$$;

--$60/1000 + 20% commission

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_OUTPUT_CONFIGURATION;

DESC TABLE CUSTOMER.FILE_PROCESSING.OUTPUT_GENDER_RESULTS;

{
  "attributes": [
    {
      "code": "GNDRPR",
      "label": "Gender Predictions"
    }
  ],
  "output_fields": [
    {
      "field_name": "RECORD_ID",
      "sort_order": null
    },
    {
      "field_name": "FIRST_NAME",
      "sort_order": null
    },
    {
      "field_name": "LAST_NAME",
      "sort_order": null
    },
    {
      "field_name": "ZIP",
      "sort_order": null
    },
    {
      "field_name": "MALE_PROBABILITY",
      "sort_order": 1
    },
    {
      "field_name": "FEMALE_PROBABILITY",
      "sort_order": 2
    },
    {
      "field_name": "GENDER_CONFIDENCE_LEVEL",
      "sort_order": 3
    },
    {
      "field_name": "PREDICTED_GENDER",
      "sort_order": 4
    }
  ],
  "retain_source_fields": true
}