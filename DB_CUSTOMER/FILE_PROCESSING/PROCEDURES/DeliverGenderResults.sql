SELECT * FROM  CUSTOMER.ANALYTICS.BATCH_GENDER_SUMMARY;

--FIRST_NAME_MISSING_FLAG
--SSA_IS_RARE_NAME
--SSA_MALE_PROBABILITY
--SSA_FEMALE_PROBABILITY

CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.DELIVER_GENDER_RESULTS("MODEL_VERSION" VARCHAR DEFAULT 'v0.01')
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    BATCH_PROCESSED_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    BATCH_COUNT NUMBER DEFAULT 0;
BEGIN

    CREATE OR REPLACE TEMP TABLE BATCHES_TO_PROCESS AS
    SELECT DISTINCT
        b.CLIENT_ID,
        b.BATCH_ID
    FROM CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE b
    INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH cb
        ON b.CLIENT_ID = cb.CLIENT_ID
       AND b.BATCH_ID = cb.ID
    WHERE cb.DELIVERY_COMPLETE_TS IS NULL;

    SELECT COUNT(*) INTO :BATCH_COUNT
    FROM BATCHES_TO_PROCESS;

    IF (:BATCH_COUNT = 0) THEN
        RETURN ''SUCCESS - NO UNPROCESSED BATCHES FOUND'';
    END IF;

    CREATE OR REPLACE TEMP TABLE BASELINE_TO_PROCESS AS
    SELECT b.*
    FROM CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE b
    INNER JOIN BATCHES_TO_PROCESS p
        ON b.CLIENT_ID = p.CLIENT_ID
       AND b.BATCH_ID = p.BATCH_ID;

    MERGE INTO CUSTOMER.FILE_PROCESSING.OUTPUT_GENDER_RESULTS tgt
    USING (
        SELECT
            CLIENT_ID,
            BATCH_ID,
            RECORD_ID,
            SSA_PREDICTED_GENDER AS PREDICTED_GENDER,
            SSA_REPORTABLE_GENDER AS REPORTABLE_GENDER,
            SSA_CONFIDENCE_LEVEL AS CONFIDENCE_LEVEL,
            SSA_MAX_PROBABILITY AS MAX_PROBABILITY,
            SSA_PROBABILITY_GAP AS PROBABILITY_GAP,
            :MODEL_VERSION AS MODEL_VERSION,
            LOAD_TS AS PROCESSED_TS,
            FIRST_NAME_MISSING_FLAG,
            SSA_IS_RARE_NAME AS IS_RARE_NAME_FLAG,
            SSA_MALE_PROBABILITY AS MALE_PROBABILITY,
            SSA_FEMALE_PROBABILITY AS FEMALE_PROBABILITY
        FROM BASELINE_TO_PROCESS
    ) src
    ON tgt.CLIENT_ID = src.CLIENT_ID
   AND tgt.BATCH_ID = src.BATCH_ID
   AND tgt.RECORD_ID = src.RECORD_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.PREDICTED_GENDER = src.PREDICTED_GENDER,
        tgt.REPORTABLE_GENDER = src.REPORTABLE_GENDER,
        tgt.CONFIDENCE_LEVEL = src.CONFIDENCE_LEVEL,
        tgt.MAX_PROBABILITY = src.MAX_PROBABILITY,
        tgt.PROBABILITY_GAP = src.PROBABILITY_GAP,
        tgt.MODEL_VERSION = src.MODEL_VERSION,
        tgt.FIRST_NAME_MISSING_FLAG = src.FIRST_NAME_MISSING_FLAG.
        tgt.IS_RARE_NAME_FLAG = src.IS_RARE_NAME_FLAG,
        tgt.SSA_MALE_PROBABILITY = src.MALE_PROBABILITY,
        tgt.SSA_FEMALE_PROBABILITY = src.FEMALE_PROBABILITY,
        tgt.PROCESSED_TS = src.PROCESSED_TS
    WHEN NOT MATCHED THEN INSERT (
        CLIENT_ID,
        BATCH_ID,
        RECORD_ID,
        PREDICTED_GENDER,
        REPORTABLE_GENDER,
        CONFIDENCE_LEVEL,
        MAX_PROBABILITY,
        PROBABILITY_GAP,
        MODEL_VERSION,
        PROCESSED_TS,
        FIRST_NAME_MISSING_FLAG,
        IS_RARE_NAME_FLAG,
        MALE_PROBABILITY,
        FEMALE_PROBABILITY
    )
    VALUES (
        src.CLIENT_ID,
        src.BATCH_ID,
        src.RECORD_ID,
        src.PREDICTED_GENDER,
        src.REPORTABLE_GENDER,
        src.CONFIDENCE_LEVEL,
        src.MAX_PROBABILITY,
        src.PROBABILITY_GAP,
        src.MODEL_VERSION,
        src.PROCESSED_TS,
        src.FIRST_NAME_MISSING_FLAG,
        src.IS_RARE_NAME_FLAG,
        src.MALE_PROBABILITY,
        src.FEMALE_PROBABILITY
    );


-- METRIC TO SUMMARY TABLE
    MERGE INTO CUSTOMER.FILE_PROCESSING.GENDER_RESULTS_BATCH_SUMMARY tgt    
    USING (
         -- gender breakdown
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Gender'' AS METRIC_NAME,
                ''COUNT'' AS METRIC_TYPE,
                b.REPORTABLE_GENDER AS METRIC_LABEL,
                COUNT(*) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                b.REPORTABLE_GENDER
            UNION ALL
            -- all confidence levels, non-hardcoded
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Confidence Level'' AS METRIC_NAME,
                ''COUNT'' AS METRIC_TYPE,
                b.CONFIDENCE_LEVEL AS METRIC_LABEL,
                COUNT(*) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                b.CONFIDENCE_LEVEL
            UNION ALL
            -- rare-name confidence breakdown, non-hardcoded
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Uncommon Name Confidence Level'' AS METRIC_NAME,
                ''COUNT'' AS METRIC_TYPE,
                b.CONFIDENCE_LEVEL AS METRIC_LABEL,
                COUNT(*) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            WHERE b.IS_RARE_NAME_FLAG = 1
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                b.CONFIDENCE_LEVEL
            UNION ALL
             -- Rare name count
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Uncommon Names'' AS METRIC_NAME,
                ''COUNT'' AS METRIC_TYPE,
                ''TOTAL_UNCOMMON_NAMES'' AS METRIC_LABEL,
                COUNT(*) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            WHERE IS_RARE_NAME_FLAG = 1
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION
            UNION ALL
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Total Records'' AS METRIC_NAME,
                ''COUNT'' AS METRIC_TYPE,
                ''TOTAL_RECORDS'' AS METRIC_LABEL,
                COUNT(*) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION
            UNION ALL
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Missing First Name'' AS METRIC_NAME,
                ''COUNT'' AS METRIC_TYPE,
                ''MISSING_FIRST_NAME'' AS METRIC_LABEL,
                COUNT(*) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            WHERE FIRST_NAME_MISSING_FLAG = 1
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION 
            UNION ALL
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Average Max Probability'' AS METRIC_NAME,
                ''AVERAGE'' AS METRIC_TYPE,
                ''AVERAGE_MAX_PROBABILITY'' AS METRIC_LABEL,
                AVG(b.MAX_PROBABILITY) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION
            UNION ALL
            SELECT
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION,
                ''Average Probability Gap'' AS METRIC_NAME,
                ''AVERAGE'' AS METRIC_TYPE,
                ''AVERAGE_PROBABILITY_GAP'' AS METRIC_LABEL,
                 AVG(b.PROBABILITY_GAP) AS METRIC_VALUE,
                MAX(b.PROCESSED_TS) AS PROCESSED_TS
            FROM BASE b
            GROUP BY
                b.CLIENT_ID,
                b.BATCH_ID,
                b.MODEL_VERSION
    ) AS src
    ON tgt.CLIENT_ID = src.CLIENT_ID
        AND tgt.BATCH_ID = src.BATCH_ID
        AND tgt.MODEL_VERSION = src.MODEL_VERSION
        AND tgt.METRIC_NAME = src.METRIC_NAME
        AND tgt.METRIC_TYPE = src.METRIC_TYPE
        AND tgt.METRIC_LABEL = src.METRIC_LABEL
    WHEN MATCHED THEN UPDATE SET
        tgt.METRIC_VALUE = src.METRIC_VALUE,
        tgt.PROCESSED_TS = src.PROCESSED_TS
    WHEN NOT MATCHED THEN INSERT (
        CLIENT_ID,
        BATCH_ID,
        MODEL_VERSION,
        METRIC_NAME,
        METRIC_TYPE,
        METRIC_VALUE,
        METRIC_LABEL,
        PROCESSED_TS
    )
    VALUES (
        src.CLIENT_ID,
        src.BATCH_ID,
        src.MODEL_VERSION,
        src.METRIC_NAME,
        src.METRIC_TYPE,
        src.METRIC_VALUE,
        src.METRIC_LABEL,
        src.PROCESSED_TS
    );

    MERGE INTO INFERLYTICA.CLIENT_MODEL_IMPROVEMENT.GENDER_OUTPUT_MODEL_RESULTS tgt
    USING (
        SELECT
            b.CLIENT_ID,
            b.BATCH_ID,
            b.RECORD_ID,
            b.FIRST_NAME_RAW,
            b.FIRST_NAME_CLEAN,
            b.LAST_NAME_RAW,
            b.LAST_NAME_CLEAN,
            b.ZIP_RAW,
            b.ZIP5,
            b.SSA_MALE_PROBABILITY,
            b.SSA_FEMALE_PROBABILITY,
            b.SSA_CONFIDENCE_LEVEL,
            b.SSA_LOOKUP_MATCH_FLAG,
            b.SSA_PREDICTED_GENDER,
            b.SSA_REPORTABLE_GENDER,
            :MODEL_VERSION AS MODEL_VERSION
        FROM BASELINE_TO_PROCESS b
        INNER JOIN CUSTOMER.MANAGEMENT.CLIENT c
            ON b.CLIENT_ID = c.ID
        WHERE c.MODEL_IMPROVEMENT_PROGRAM_PARTICIPANT = TRUE
    ) src
    ON tgt.CLIENT_ID = src.CLIENT_ID
   AND tgt.BATCH_ID = src.BATCH_ID
   AND tgt.RECORD_ID = src.RECORD_ID
    WHEN MATCHED THEN UPDATE SET
        tgt.FIRST_NAME_RAW = src.FIRST_NAME_RAW,
        tgt.FIRST_NAME_CLEAN = src.FIRST_NAME_CLEAN,
        tgt.LAST_NAME_RAW = src.LAST_NAME_RAW,
        tgt.LAST_NAME_CLEAN = src.LAST_NAME_CLEAN,
        tgt.ZIP_RAW = src.ZIP_RAW,
        tgt.ZIP5 = src.ZIP5,
        tgt.SSA_MALE_PROBABILITY = src.SSA_MALE_PROBABILITY,
        tgt.SSA_FEMALE_PROBABILITY = src.SSA_FEMALE_PROBABILITY,
        tgt.SSA_CONFIDENCE_LEVEL = src.SSA_CONFIDENCE_LEVEL,
        tgt.SSA_LOOKUP_MATCH_FLAG = src.SSA_LOOKUP_MATCH_FLAG,
        tgt.SSA_PREDICTED_GENDER = src.SSA_PREDICTED_GENDER,
        tgt.SSA_REPORTABLE_GENDER = src.SSA_REPORTABLE_GENDER,
        tgt.MODEL_VERSION = src.MODEL_VERSION
    WHEN NOT MATCHED THEN INSERT (
        CLIENT_ID,
        BATCH_ID,
        RECORD_ID,
        FIRST_NAME_RAW,
        FIRST_NAME_CLEAN,
        LAST_NAME_RAW,
        LAST_NAME_CLEAN,
        ZIP_RAW,
        ZIP5,
        SSA_MALE_PROBABILITY,
        SSA_FEMALE_PROBABILITY,
        SSA_CONFIDENCE_LEVEL,
        SSA_LOOKUP_MATCH_FLAG,
        SSA_PREDICTED_GENDER,
        SSA_REPORTABLE_GENDER,
        MODEL_VERSION
    )
    VALUES (
        src.CLIENT_ID,
        src.BATCH_ID,
        src.RECORD_ID,
        src.FIRST_NAME_RAW,
        src.FIRST_NAME_CLEAN,
        src.LAST_NAME_RAW,
        src.LAST_NAME_CLEAN,
        src.ZIP_RAW,
        src.ZIP5,
        src.SSA_MALE_PROBABILITY,
        src.SSA_FEMALE_PROBABILITY,
        src.SSA_CONFIDENCE_LEVEL,
        src.SSA_LOOKUP_MATCH_FLAG,
        src.SSA_PREDICTED_GENDER,
        src.SSA_REPORTABLE_GENDER,
        src.MODEL_VERSION
    );

   

    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH cb
    SET DELIVERY_COMPLETE_TS = :BATCH_PROCESSED_TS
    WHERE EXISTS (
        SELECT 1
        FROM BATCHES_TO_PROCESS p
        WHERE p.CLIENT_ID = cb.CLIENT_ID
          AND p.BATCH_ID = cb.ID
    );

    MERGE INTO CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS tgt
    USING (
        SELECT
            p.CLIENT_ID,
            p.BATCH_ID,
            cb.APP_FILE_ID,
            c.APP_ORGANIZATION_ID,
            TO_JSON(
                OBJECT_CONSTRUCT(
                    ''organization_id'', c.APP_ORGANIZATION_ID,
                    ''batch_id'', cb.APP_FILE_ID,
                    ''delivery_status'', ''READY'',
                    ''delivery_status_updated_at'', CURRENT_TIMESTAMP()
                )
            ) AS PAYLOAD,
            ''DELIVERY_STATUS_UPDATED'' AS EVENT_TYPE,
            ''READY'' AS STATUS,
            0 AS ATTEMPT_COUNT,
            NULL AS LAST_ATTEMPT_TS,
            NULL AS LAST_ERROR,
            CURRENT_TIMESTAMP() AS CREATED_TS,
            NULL AS PROCESSED_TS
        FROM BATCHES_TO_PROCESS p
        INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH cb
            ON p.CLIENT_ID = cb.CLIENT_ID
           AND p.BATCH_ID = cb.ID
        INNER JOIN CUSTOMER.MANAGEMENT.CLIENT c
            ON p.CLIENT_ID = c.ID
    ) src
    ON tgt.CLIENT_ID = src.CLIENT_ID
    AND tgt.BATCH_ID = src.BATCH_ID
    AND tgt.EVENT_TYPE = src.EVENT_TYPE
    AND tgt.PROCESSED_TS IS NULL
    WHEN MATCHED THEN UPDATE SET
        tgt.APP_FILE_ID = src.APP_FILE_ID,
        tgt.APP_ORGANIZATION_ID = src.APP_ORGANIZATION_ID,
        tgt.PAYLOAD = src.PAYLOAD,
        tgt.STATUS = src.STATUS
    WHEN NOT MATCHED THEN INSERT
    (
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
        CREATED_TS,
        PROCESSED_TS
    )
    VALUES
    (
        src.CLIENT_ID,
        src.BATCH_ID,
        src.APP_FILE_ID,
        src.APP_ORGANIZATION_ID,
        src.PAYLOAD,
        src.EVENT_TYPE,
        src.STATUS,
        src.ATTEMPT_COUNT,
        src.LAST_ATTEMPT_TS,
        src.LAST_ERROR,
        src.CREATED_TS,
        src.PROCESSED_TS
    );

    DELETE FROM CUSTOMER.RAW.PERSON_INPUT r
    WHERE EXISTS (
        SELECT 1
        FROM BATCHES_TO_PROCESS p
        WHERE p.CLIENT_ID = r.CLIENT_ID
          AND p.BATCH_ID = r.BATCH_ID
    );

    ALTER DYNAMIC TABLE CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE REFRESH;

    RETURN ''SUCCESS - PROCESSED '' || :BATCH_COUNT || '' BATCH(ES)'';
END;
';

DESC TABLE  CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS;


MERGE INTO CUSTOMER.ANALYTICS.BATCH_GENDER_SUMMARY_BREAKDOWN tgt
USING (
    WITH BASE AS (
        SELECT
            CLIENT_ID,
            BATCH_ID,
            MODEL_VERSION,
            REPORTABLE_GENDER,
            CONFIDENCE_LEVEL,
            IS_RARE_NAME_FLAG
        FROM CUSTOMER.FILE_PROCESSING.OUTPUT_GENDER_RESULTS
    ),
    TOTALS AS (
        SELECT
            CLIENT_ID,
            BATCH_ID,
            MODEL_VERSION,
            COUNT(*) AS TOTAL_RECORDS
        FROM BASE
        GROUP BY CLIENT_ID, BATCH_ID, MODEL_VERSION
    ),
    BREAKDOWN_ROWS AS (

        -- gender breakdown
        SELECT
            b.CLIENT_ID,
            b.BATCH_ID,
            b.MODEL_VERSION,
            'REPORTABLE_GENDER' AS METRIC_TYPE,
            b.REPORTABLE_GENDER AS METRIC_VALUE,
            COUNT(*) AS RECORD_COUNT
        FROM BASE b
        GROUP BY
            b.CLIENT_ID,
            b.BATCH_ID,
            b.MODEL_VERSION,
            b.REPORTABLE_GENDER

        UNION ALL

        -- all confidence levels, non-hardcoded
        SELECT
            b.CLIENT_ID,
            b.BATCH_ID,
            b.MODEL_VERSION,
            'CONFIDENCE_LEVEL' AS METRIC_TYPE,
            b.CONFIDENCE_LEVEL AS METRIC_VALUE,
            COUNT(*) AS RECORD_COUNT
        FROM BASE b
        GROUP BY
            b.CLIENT_ID,
            b.BATCH_ID,
            b.MODEL_VERSION,
            b.CONFIDENCE_LEVEL

        UNION ALL

        -- rare-name confidence breakdown, non-hardcoded
        SELECT
            b.CLIENT_ID,
            b.BATCH_ID,
            b.MODEL_VERSION,
            'RARE_NAME_CONFIDENCE' AS METRIC_TYPE,
            b.CONFIDENCE_LEVEL AS METRIC_VALUE,
            COUNT(*) AS RECORD_COUNT
        FROM BASE b
        WHERE b.IS_RARE_NAME_FLAG = 1
        GROUP BY
            b.CLIENT_ID,
            b.BATCH_ID,
            b.MODEL_VERSION,
            b.CONFIDENCE_LEVEL
    )
    SELECT
        r.CLIENT_ID,
        r.BATCH_ID,
        r.MODEL_VERSION,
        r.METRIC_TYPE,
        r.METRIC_VALUE,
        r.RECORD_COUNT,
        r.RECORD_COUNT / NULLIF(t.TOTAL_RECORDS, 0) AS RECORD_RATE,
        CURRENT_TIMESTAMP() AS PROCESSED_TS
    FROM BREAKDOWN_ROWS r
    INNER JOIN TOTALS t
        ON r.CLIENT_ID = t.CLIENT_ID
       AND r.BATCH_ID = t.BATCH_ID
       AND r.MODEL_VERSION = t.MODEL_VERSION
) src
ON tgt.CLIENT_ID = src.CLIENT_ID
AND tgt.BATCH_ID = src.BATCH_ID
AND tgt.MODEL_VERSION = src.MODEL_VERSION
AND tgt.METRIC_TYPE = src.METRIC_TYPE
AND COALESCE(tgt.METRIC_VALUE, '') = COALESCE(src.METRIC_VALUE, '')
WHEN MATCHED THEN UPDATE SET
    tgt.RECORD_COUNT = src.RECORD_COUNT,
    tgt.RECORD_RATE = src.RECORD_RATE,
    tgt.PROCESSED_TS = src.PROCESSED_TS
WHEN NOT MATCHED THEN INSERT (
    CLIENT_ID,
    BATCH_ID,
    MODEL_VERSION,
    METRIC_TYPE,
    METRIC_VALUE,
    RECORD_COUNT,
    RECORD_RATE,
    PROCESSED_TS
)
VALUES (
    src.CLIENT_ID,
    src.BATCH_ID,
    src.MODEL_VERSION,
    src.METRIC_TYPE,
    src.METRIC_VALUE,
    src.RECORD_COUNT,
    src.RECORD_RATE,
    src.PROCESSED_TS
);