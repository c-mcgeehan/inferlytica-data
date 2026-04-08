CREATE OR REPLACE TASK CUSTOMER.MANAGEMENT.TRIGGER_CREDIT_APPROVAL_REQUEST_TASK
  USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 10
  SERVERLESS_TASK_MAX_STATEMENT_SIZE = XSMALL
  TARGET_COMPLETION_INTERVAL = '15 MINUTES'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER.MANAGEMENT.CREDIT_APPROVAL_QUEUE_STREAM')
AS
CALL CUSTOMER.MANAGEMENT.SEND_SUPABASE_APPROVAL_REQUEST();

ALTER TASK CUSTOMER.MANAGEMENT.TRIGGER_CREDIT_APPROVAL_REQUEST_TASK RESUME;

SELECT SYSTEM$STREAM_HAS_DATA('CUSTOMER.MANAGEMENT.CREDIT_APPROVAL_QUEUE_STREAM');

SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TRIGGER_CREDIT_APPROVAL_REQUEST_TASK',
    RESULT_LIMIT => 50
  )
)
ORDER BY SCHEDULED_TIME DESC;

Python Interpreter Error:
Traceback (most recent call last):
  File "_udf_code.py", line 59, in main
    raise Exception(
Exception: Supabase approval request failed. Status: 401. Body: {"error":"Unauthorized"}
 in function SEND_SUPABASE_APPROVAL_REQUEST with handler main
Python Interpreter Error:
Traceback (most recent call last):
  File "_udf_code.py", line 8, in main
    token = get_generic_secret_string("webhook_secret")
            ^^^^^^^^^^^^^^^^^^^^^^^^^
NameError: name 'get_generic_secret_string' is not defined
 in function SEND_SUPABASE_APPROVAL_REQUEST with handler main