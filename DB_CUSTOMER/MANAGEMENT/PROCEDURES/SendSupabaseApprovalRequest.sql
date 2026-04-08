CREATE OR REPLACE PROCEDURE CUSTOMER.MANAGEMENT.SEND_SUPABASE_APPROVAL_REQUEST()
COPY GRANTS
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (CUSTOMER_FILE_PROCESSING_SUPABASE_EDGE_ACCESS_INTEGRATION)
SECRETS = ('webhook_secret' = CUSTOMER.MANAGEMENT.BATCH_APPROVAL_WEBHOOK_SECRET)
PACKAGES = ('snowflake-snowpark-python', 'requests')
AS
$$
import json
import requests
from snowflake.snowpark.secrets import get_generic_secret_string

def main(session):
    token = get_generic_secret_string("webhook_secret")


    rows = session.sql("""
        SELECT
            Q.APP_ORGANIZATION_ID AS ORGANIZATION_ID,
            Q.APP_BATCH_ID AS BATCH_ID,
            Q.CREATED_TS AS CREATED_AT,
            MAX(B.RECORD_COUNT) AS RECORD_COUNT
        FROM CUSTOMER.MANAGEMENT.CREDIT_APPROVAL_QUEUE_STREAM Q
        INNER JOIN CUSTOMER.FILE_PROCESSING.CLIENT_BATCH B
            ON B.APP_BATCH_ID = Q.APP_BATCH_ID
        WHERE Q.STATUS = 'PENDING'
        GROUP BY
            Q.APP_ORGANIZATION_ID,
            Q.APP_BATCH_ID,
            Q.CREATED_TS
    """).collect()

    session.sql("""
        INSERT INTO CUSTOMER.MANAGEMENT.STREAM_DUMP
        SELECT 'DUMP'
        FROM CUSTOMER.MANAGEMENT.CREDIT_APPROVAL_QUEUE_STREAM
    """).collect()

    if not rows:
        return 'No pending approval requests found in stream.'

    payload = [
        {
            "record_count": int(row["RECORD_COUNT"]) if row["RECORD_COUNT"] is not None else 0,
            "organization_id": row["ORGANIZATION_ID"],
            "batch_id": row["BATCH_ID"],
            "created_at": row["CREATED_AT"].isoformat() if row["CREATED_AT"] is not None else None,
        }
        for row in rows
    ]

    url = 'https://agslmpzenhwusxizgpfe.supabase.co/functions/v1/batch-approval-request'

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    response = requests.post(
        url,
        headers=headers,
        data=json.dumps(payload),
        timeout=30
    )

    if response.status_code >= 400:
        raise Exception(
            f'Supabase approval request failed. '
            f'Status: {response.status_code}. Body: {response.text}'
        )

    return f'Sent {len(payload)} batch approval request(s) to Supabase.'
$$;