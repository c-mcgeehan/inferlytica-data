DESC TABLE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE;

CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.UPDATE_SUPABASE_BATCH_DOWNLOAD()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (CUSTOMER_FILE_PROCESSING_SUPABASE_EDGE_ACCESS_INTEGRATION)
SECRETS = ('webhook_secret' = CUSTOMER.FILE_PROCESSING.DOWNLOAD_STATUS_WEBHOOK_SECRET)
EXECUTE AS CALLER
AS
$$
import requests
from snowflake.snowpark import Session
from snowflake.snowpark.secrets import get_generic_secret_string

FUNCTION_URL = "https://agslmpzenhwusxizgpfe.supabase.co/functions/v1/update-upload-download-status"

def run(session: Session):
    token = get_generic_secret_string("webhook_secret")

    success_count = 0
    failure_count = 0
    processed_count = 0

    try:
        session.sql("BEGIN").collect()

        session.sql("""
            CREATE OR REPLACE TEMP TABLE CUSTOMER.FILE_PROCESSING.TMP_CLIENT_BATCH_DOWNLOAD_READY_QUEUE_STREAM AS
            SELECT
                APP_ORGANIZATION_ID,
                APP_BATCH_ID,
                DOWNLOAD_OBJECT_KEY,
                DOWNLOAD_STATUS,
                QUEUED_TS,
                PROCESSING_STATUS,
                PROCESSING_STARTED_TS,
                PROCESSED_TS,
                ERROR_MESSAGE,
                CREATED_TS,
                UPDATED_TS
            FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE_STREAM
            WHERE PROCESSING_STATUS = 'PENDING'
        """).collect()

        rows = session.sql("""
            SELECT
                APP_ORGANIZATION_ID,
                APP_BATCH_ID,
                DOWNLOAD_OBJECT_KEY,
                DOWNLOAD_STATUS
            FROM CUSTOMER.FILE_PROCESSING.TMP_CLIENT_BATCH_DOWNLOAD_READY_QUEUE_STREAM
            ORDER BY APP_ORGANIZATION_ID, APP_BATCH_ID
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
            app_organization_id = row["APP_ORGANIZATION_ID"]
            app_batch_id = row["APP_BATCH_ID"]
            download_object_key = row["DOWNLOAD_OBJECT_KEY"]
            download_status = row["DOWNLOAD_STATUS"]

            try:
                session.sql(
                    """
                    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE
                    SET
                        PROCESSING_STATUS = ?,
                        PROCESSING_STARTED_TS = CURRENT_TIMESTAMP(),
                        UPDATED_TS = CURRENT_TIMESTAMP(),
                        ERROR_MESSAGE = NULL
                    WHERE APP_ORGANIZATION_ID = ?
                      AND APP_BATCH_ID = ?
                      AND PROCESSING_STATUS = 'PENDING'
                    """,
                    params=["IN_PROGRESS", app_organization_id, app_batch_id]
                ).collect()

                payload = {
                    "organization_id": app_organization_id,
                    "batch_id": app_batch_id,
                    "object_key": download_object_key,
                    "status": download_status,
                }

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
                        UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE
                        SET
                            PROCESSING_STATUS = ?,
                            PROCESSED_TS = CURRENT_TIMESTAMP(),
                            UPDATED_TS = CURRENT_TIMESTAMP(),
                            ERROR_MESSAGE = NULL
                        WHERE APP_ORGANIZATION_ID = ?
                          AND APP_BATCH_ID = ?
                        """,
                        params=["SUCCEEDED", app_organization_id, app_batch_id]
                    ).collect()
                    success_count += 1
                else:
                    session.sql(
                        """
                        UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE
                        SET
                            PROCESSING_STATUS = ?,
                            UPDATED_TS = CURRENT_TIMESTAMP(),
                            ERROR_MESSAGE = ?
                        WHERE APP_ORGANIZATION_ID = ?
                          AND APP_BATCH_ID = ?
                        """,
                        params=[
                            "FAILED",
                            f"HTTP {response.status_code}: {response_text}",
                            app_organization_id,
                            app_batch_id
                        ]
                    ).collect()
                    failure_count += 1

            except Exception as row_error:
                session.sql(
                    """
                    UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_DOWNLOAD_READY_QUEUE
                    SET
                        PROCESSING_STATUS = ?,
                        UPDATED_TS = CURRENT_TIMESTAMP(),
                        ERROR_MESSAGE = ?
                    WHERE APP_ORGANIZATION_ID = ?
                      AND APP_BATCH_ID = ?
                    """,
                    params=[
                        "FAILED",
                        str(row_error)[:5000],
                        app_organization_id,
                        app_batch_id
                    ]
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