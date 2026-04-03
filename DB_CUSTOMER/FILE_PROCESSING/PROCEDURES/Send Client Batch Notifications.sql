CREATE OR REPLACE PROCEDURE CUSTOMER.FILE_PROCESSING.SEND_CLIENT_BATCH_NOTIFICATIONS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (CUSTOMER_FILE_PROCESSING_SUPABASE_EDGE_ACCESS_INTEGRATION)
SECRETS = ('webhook_secret' = CUSTOMER.FILE_PROCESSING.UPLOAD_DELIVERY_WEBHOOK_SECRET)
EXECUTE AS OWNER
AS
$$
import json
import requests
from snowflake.snowpark import Session
from snowflake.snowpark.secrets import get_generic_secret_string

FUNCTION_URL = "https://agslmpzenhwusxizgpfe.supabase.co/functions/v1/update-upload-delivery"

def run(session: Session):
    token = get_generic_secret_string("webhook_secret")

    rows = session.sql("""
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
            LAST_ERROR
        FROM TMP_CLIENT_BATCH_NOTIFICATIONS
        ORDER BY ID
    """).collect()

    success_count = 0
    failure_count = 0

    for row in rows:
        notification_id = row["ID"]
        payload_raw = row["PAYLOAD"]

        try:
            session.sql(
                """
                UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
                SET
                    STATUS = ?,
                    ATTEMPT_COUNT = ATTEMPT_COUNT + 1,
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

        except Exception as e:
            session.sql(
                """
                UPDATE CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS
                SET
                    STATUS = ?,
                    LAST_ERROR = ?
                WHERE ID = ?
                """,
                params=["FAILED", str(e)[:5000], notification_id]
            ).collect()
            failure_count += 1

    return {
        "processed": len(rows),
        "succeeded": success_count,
        "failed": failure_count
    }
$$;