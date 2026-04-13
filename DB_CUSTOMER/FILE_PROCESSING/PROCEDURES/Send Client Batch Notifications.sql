--CALL CUSTOMER.FILE_PROCESSING.SEND_CLIENT_BATCH_NOTIFICATIONS();

SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
ORDER BY BATCH_ID DESC;
{"batch_id":"e43ae6db-fdc6-4d82-9aaf-f8bd1a27269c","delivery_status":"READY","delivery_status_updated_at":"2026-04-10 15:37:39.025 -0700","file_analysis":[{"analysis_type":"gender_metrics","metrics":[{"metric_display_type":"INTEGER","metric_label_sort":"VALUE","metric_name":"Gender","metric_sort":1,"metric_type":"COUNT","metrics":[{"label":"F","value":48},{"label":"M","value":50},{"label":"U","value":2}]},{"metric_display_type":"INTEGER","metric_label_sort":"CONFIDENCE","metric_name":"Confidence Level","metric_sort":2,"metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":1},{"label":"HIGH","value":97},{"label":"LOW","value":1},{"label":"MEDIUM","value":1}]},{"metric_display_type":"INTEGER","metric_name":"Uncommon Names","metric_sort":3,"metric_type":"COUNT","metrics":[{"label":"TOTAL_UNCOMMON_NAMES","value":0}]},{"metric_display_type":"INTEGER","metric_name":"Missing First Name","metric_sort":4,"metric_type":"COUNT","metrics":[{"label":"MISSING_FIRST_NAME","value":0}]},{"metric_display_type":"PERCENT","metric_name":"Average Max Probability","metric_sort":5,"metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_MAX_PROBABILITY","value":0.981638}]},{"metric_display_type":"PERCENT","metric_name":"Average Probability Gap","metric_sort":6,"metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_PROBABILITY_GAP","value":0.963277}]}]}],"organization_id":"1d96f728-bdc7-4554-8167-89cbd072b075","processed_record_count":100}


CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.SEND_CLIENT_BATCH_NOTIFICATIONS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (CUSTOMER_FILE_PROCESSING_SUPABASE_EDGE_ACCESS_INTEGRATION)
SECRETS = ('webhook_secret' = CUSTOMER.FILE_PROCESSING.UPLOAD_DELIVERY_WEBHOOK_SECRET)
EXECUTE AS CALLER
AS
$$
import json
import requests
from snowflake.snowpark import Session
from snowflake.snowpark.secrets import get_generic_secret_string

FUNCTION_URL = "https://agslmpzenhwusxizgpfe.supabase.co/functions/v1/update-upload-delivery"

def run(session: Session):
    token = get_generic_secret_string("webhook_secret")

    success_count = 0
    failure_count = 0
    processed_count = 0

    try:
        # Start one explicit transaction so the stream snapshot and all updates
        # are tied to the same unit of work.
        session.sql("BEGIN").collect()

        # Snapshot the stream into a temp table in THIS SAME session/transaction.
        session.sql("""
            CREATE OR REPLACE TEMP TABLE CUSTOMER.FILE_PROCESSING.TMP_CLIENT_BATCH_NOTIFICATIONS AS
            SELECT
                ID,
                CLIENT_ID,
                BATCH_ID,
                APP_BATCH_ID,
                APP_ORGANIZATION_ID,
                PAYLOAD,
                EVENT_TYPE,
                STATUS,
                ATTEMPT_COUNT,
                LAST_ATTEMPT_TS,
                LAST_ERROR
            FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS_STREAM
            WHERE METADATA$ACTION = 'INSERT'
        """).collect()

        rows = session.sql("""
            SELECT
                ID,
                CLIENT_ID,
                BATCH_ID,
                APP_BATCH_ID,
                APP_ORGANIZATION_ID,
                PAYLOAD,
                EVENT_TYPE,
                STATUS,
                ATTEMPT_COUNT,
                LAST_ATTEMPT_TS,
                LAST_ERROR
            FROM CUSTOMER.FILE_PROCESSING.TMP_CLIENT_BATCH_NOTIFICATIONS
            ORDER BY ID
        """).collect()

        processed_count = len(rows)

        if processed_count == 0:
            session.sql("COMMIT").collect()
            return {
                "processed": 0,
                "succeeded": 0,
                "failed": 0,
                "message": "No stream rows to process."
            }

        for row in rows:
            notification_id = row["ID"]
            payload_raw = row["PAYLOAD"]

            try:
                session.sql(
                    """
                    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
                    SET
                        STATUS = ?,
                        ATTEMPT_COUNT = COALESCE(ATTEMPT_COUNT, 0) + 1,
                        LAST_ATTEMPT_TS = CURRENT_TIMESTAMP(),
                        LAST_ERROR = NULL
                    WHERE ID = ?
                    """,
                    params=["IN_PROGRESS", notification_id]
                ).collect()

                if isinstance(payload_raw, str):
                    payload = json.loads(payload_raw)
                else:
                    payload = payload_raw

                response = requests.post(
                    FUNCTION_URL,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json"
                    },
                    json=payload,
                    timeout=30
                )

                response_text = response.text[:5000]

                if 200 <= response.status_code < 300:
                    session.sql(
                        """
                        UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
                        SET
                            STATUS = ?,
                            PROCESSED_TS = CURRENT_TIMESTAMP(),
                            LAST_ERROR = NULL
                        WHERE ID = ?
                        """,
                        params=["SUCCEEDED", notification_id]
                    ).collect()
                    success_count += 1
                else:
                    session.sql(
                        """
                        UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
                        SET
                            STATUS = ?,
                            LAST_ERROR = ?
                        WHERE ID = ?
                        """,
                        params=["FAILED", f"HTTP {response.status_code}: {response_text}", notification_id]
                    ).collect()
                    failure_count += 1

            except Exception as row_error:
                session.sql(
                    """
                    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
                    SET
                        STATUS = ?,
                        LAST_ERROR = ?
                    WHERE ID = ?
                    """,
                    params=["FAILED", str(row_error)[:5000], notification_id]
                ).collect()
                failure_count += 1

        session.sql("COMMIT").collect()

        return {
            "processed": processed_count,
            "succeeded": success_count,
            "failed": failure_count
        }

    except Exception as fatal_error:
        try:
            session.sql("ROLLBACK").collect()
        except Exception:
            pass

        raise fatal_error
$$;