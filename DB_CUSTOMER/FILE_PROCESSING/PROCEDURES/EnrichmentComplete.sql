SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS;

DESC TABLE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;

SELECT *
FROm CUSTOMER.FILE_PROCESSING.CLIENT_BATCH;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_COMPLETION_QUEUE;

SELECT *
FROM CUSTOMER.RAW.PERSON_INPUT
WHERE BATCH_ID = 2501;


SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH p
INNER JOIN (
        SELECT
            CLIENT_ID,
            BATCH_ID,
            COUNT(*) AS RECORD_COUNT
        FROM CUSTOMER.RAW.PERSON_INPUT
        GROUP BY
            CLIENT_ID,
            BATCH_ID
    ) r
        ON r.CLIENT_ID = p.CLIENT_ID
       AND r.BATCH_ID = p.ID
WHERE p.ID = 2402;

CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.ENRICHMENT_COMPLETE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    BATCH_PROCESSED_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    BATCH_COUNT NUMBER DEFAULT 0;
BEGIN

    CREATE OR REPLACE TEMP TABLE BATCHES_TO_COMPLETE AS
    SELECT DISTINCT
        S.CLIENT_ID,
        S.BATCH_ID
    FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_COMPLETION_QUEUE_STREAM S;

    SELECT COUNT(*) INTO :BATCH_COUNT
    FROM BATCHES_TO_COMPLETE;

    IF (:BATCH_COUNT = 0) THEN
        RETURN ''SUCCESS - NO COMPLETED BATCHES FOUND'';
    END IF;

    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_COMPLETION_QUEUE Q
    SET
        PROCESSING_STATUS = ''RUNNING'',
        PROCESSING_STARTED_TS = COALESCE(Q.PROCESSING_STARTED_TS, :BATCH_PROCESSED_TS),
        UPDATED_TS = :BATCH_PROCESSED_TS,
        ERROR_MESSAGE = NULL
        WHERE EXISTS (
            SELECT 1
            FROM BATCHES_TO_COMPLETE B
            WHERE B.CLIENT_ID = Q.CLIENT_ID
              AND B.BATCH_ID = Q.BATCH_ID
        )
      AND Q.PROCESSING_STATUS = ''PENDING'';

    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH cb
    SET
        DELIVERY_COMPLETE_TS = :BATCH_PROCESSED_TS,
        PROCESSED_RECORD_COUNT = r.RECORD_COUNT
    FROM BATCHES_TO_COMPLETE p
    INNER JOIN (
        SELECT
            CLIENT_ID,
            BATCH_ID,
            COUNT(*) AS RECORD_COUNT
        FROM CUSTOMER.RAW.PERSON_INPUT
        GROUP BY
            CLIENT_ID,
            BATCH_ID
    ) r
        ON r.CLIENT_ID = p.CLIENT_ID
       AND r.BATCH_ID = p.BATCH_ID
    WHERE cb.CLIENT_ID = p.CLIENT_ID
      AND cb.ID = p.BATCH_ID;

    MERGE INTO CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS tgt
    USING (
        WITH FILE_ANALYSIS AS (
            SELECT
                s.CLIENT_ID,
                s.BATCH_ID,
                ARRAY_AGG(
                    TRY_PARSE_JSON(s.ENRICHMENT_METRICS_PAYLOAD)
                ) WITHIN GROUP (ORDER BY s.ENRICHMENT_TYPE_CODE) AS FILE_ANALYSIS
            FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_ENRICHMENT_STATUS s
            INNER JOIN BATCHES_TO_COMPLETE b
                ON s.CLIENT_ID = b.CLIENT_ID
               AND s.BATCH_ID = b.BATCH_ID
            WHERE s.ENRICHMENT_STATUS = ''COMPLETED''
              AND s.ENRICHMENT_METRICS_PAYLOAD IS NOT NULL
              AND TRY_PARSE_JSON(s.ENRICHMENT_METRICS_PAYLOAD) IS NOT NULL
            GROUP BY
                s.CLIENT_ID,
                s.BATCH_ID
        )
        SELECT
            cb.CLIENT_ID,
            cb.ID AS BATCH_ID,
            cb.APP_BATCH_ID,
            c.APP_ORGANIZATION_ID,
            TO_JSON(
                OBJECT_CONSTRUCT(
                    ''organization_id'', c.APP_ORGANIZATION_ID,
                    ''batch_id'', cb.APP_BATCH_ID,
                    ''delivery_status'', ''READY'',
                    ''delivery_status_updated_at'', CURRENT_TIMESTAMP(),
                    ''processed_record_count'', cb.PROCESSED_RECORD_COUNT,
                    ''file_analysis'', COALESCE(fa.FILE_ANALYSIS, ARRAY_CONSTRUCT())
                )
            ) AS PAYLOAD,
            ''DELIVERY_STATUS_UPDATED'' AS EVENT_TYPE,
            ''READY'' AS STATUS,
            0 AS ATTEMPT_COUNT,
            NULL AS LAST_ATTEMPT_TS,
            NULL AS LAST_ERROR,
            CURRENT_TIMESTAMP() AS CREATED_TS,
            NULL AS PROCESSED_TS
        FROM BATCHES_TO_COMPLETE b
        INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH cb
            ON b.CLIENT_ID = cb.CLIENT_ID
           AND b.BATCH_ID = cb.ID
        INNER JOIN CUSTOMER.MANAGEMENT.CLIENT c
            ON b.CLIENT_ID = c.ID
        LEFT JOIN FILE_ANALYSIS fa
            ON b.CLIENT_ID = fa.CLIENT_ID
           AND b.BATCH_ID = fa.BATCH_ID
    ) src
    ON tgt.CLIENT_ID = src.CLIENT_ID
    AND tgt.BATCH_ID = src.BATCH_ID
    AND tgt.EVENT_TYPE = src.EVENT_TYPE
    AND tgt.PROCESSED_TS IS NULL
    WHEN MATCHED THEN UPDATE SET
        tgt.APP_BATCH_ID = src.APP_BATCH_ID,
        tgt.APP_ORGANIZATION_ID = src.APP_ORGANIZATION_ID,
        tgt.PAYLOAD = src.PAYLOAD,
        tgt.STATUS = src.STATUS
    WHEN NOT MATCHED THEN INSERT
    (
        CLIENT_ID,
        BATCH_ID,
        APP_BATCH_ID,
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
        src.APP_BATCH_ID,
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

    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_COMPLETION_QUEUE Q
    SET
        PROCESSING_STATUS = ''COMPLETED'',
        PROCESSED_TS = :BATCH_PROCESSED_TS,
        UPDATED_TS = :BATCH_PROCESSED_TS,
        ERROR_MESSAGE = NULL
    WHERE EXISTS (
        SELECT 1
        FROM BATCHES_TO_COMPLETE B
        WHERE B.CLIENT_ID = Q.CLIENT_ID
          AND B.BATCH_ID = Q.BATCH_ID
    )
      AND Q.PROCESSING_STATUS = ''RUNNING'';

    --One of these rows for each type of enrichment
    ALTER DYNAMIC TABLE CUSTOMER.ANALYTICS.PERSON_INPUT_GENDER_BASELINE REFRESH;

    RETURN ''SUCCESS - PROCESSED '' || :BATCH_COUNT || '' BATCH(ES)'';

EXCEPTION
    WHEN STATEMENT_ERROR THEN
        UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_COMPLETION_QUEUE Q
        SET
            PROCESSING_STATUS = ''FAILED'',
            ERROR_MESSAGE = :SQLERRM,
            UPDATED_TS = CURRENT_TIMESTAMP()
        WHERE EXISTS (
            SELECT 1
            FROM BATCHES_TO_COMPLETE B
            WHERE B.CLIENT_ID = Q.CLIENT_ID
              AND B.BATCH_ID = Q.BATCH_ID
        )
          AND Q.PROCESSING_STATUS = ''RUNNING'';

        RETURN ''FAILED - '' || :SQLERRM;
END;
';

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_COMPLETION_QUEUE;

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS;
--HTTP 404: {"error":"No upload file found for this organization_id and batch_id"}*

--HTTP 404: {"error":"No upload file found for this organization_id and batch_id"}

{"batch_id":"e43ae6db-fdc6-4d82-9aaf-f8bd1a27269c","delivery_status":"READY","delivery_status_updated_at":"2026-04-10 15:37:39.025 -0700","file_analysis":[{"analysis_type":"gender_metrics","metrics":[{"metric_display_type":"INTEGER","metric_label_sort":"VALUE","metric_name":"Gender","metric_sort":1,"metric_type":"COUNT","metrics":[{"label":"F","value":48},{"label":"M","value":50},{"label":"U","value":2}]},{"metric_display_type":"INTEGER","metric_label_sort":"CONFIDENCE","metric_name":"Confidence Level","metric_sort":2,"metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":1},{"label":"HIGH","value":97},{"label":"LOW","value":1},{"label":"MEDIUM","value":1}]},{"metric_display_type":"INTEGER","metric_name":"Uncommon Names","metric_sort":3,"metric_type":"COUNT","metrics":[{"label":"TOTAL_UNCOMMON_NAMES","value":0}]},{"metric_display_type":"INTEGER","metric_name":"Missing First Name","metric_sort":4,"metric_type":"COUNT","metrics":[{"label":"MISSING_FIRST_NAME","value":0}]},{"metric_display_type":"PERCENT","metric_name":"Average Max Probability","metric_sort":5,"metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_MAX_PROBABILITY","value":0.981638}]},{"metric_display_type":"PERCENT","metric_name":"Average Probability Gap","metric_sort":6,"metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_PROBABILITY_GAP","value":0.963277}]}]}],"organization_id":"1d96f728-bdc7-4554-8167-89cbd072b075","processed_record_count":100}