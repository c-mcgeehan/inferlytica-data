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
FROM CUSTOMER.MANAGEMENT.CREDIT_APPROVAL_QUEUE_STREAM;

SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TRIGGER_CREDIT_APPROVAL_REQUEST_TASK',
    RESULT_LIMIT => 50
  )
)
ORDER BY SCHEDULED_TIME DESC;

CREATE TRANSIENT TABLE CUSTOMER.MANAGEMENT.STREAM_DUMP
(
    EMPTY_COL VARCHAR
);



SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.TASK_HISTORY(
    RESULT_LIMIT => 50
  )
)
ORDER BY SCHEDULED_TIME DESC;

Python Interpreter Error:
Traceback (most recent call last):
  File "_udf_code.py", line 17, in main
    """).collect()
         ^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/telemetry.py", line 295, in wrap
    result = func(*args, **kwargs)
             ^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/utils.py", line 1153, in call_wrapper
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/dataframe.py", line 780, in collect
    return self._internal_collect_with_tag_no_telemetry(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/dataframe.py", line 853, in _internal_collect_with_tag_no_telemetry
    return self._session._conn.execute(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 648, in execute
    result_set, result_meta = self.get_result_set(
                              ^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/analyzer/snowflake_plan.py", line 427, in wrap
    raise ne.with_traceback(tb) from None
  File "<site-packages>/snowflake/snowpark/_internal/analyzer/snowflake_plan.py", line 180, in wrap
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 770, in get_result_set
    result = self.run_query(
             ^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 137, in wrap
    raise ex
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 131, in wrap
    return func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 544, in run_query
    raise ex
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 529, in run_query
    results_cursor = self.execute_and_notify_query_listener(
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 459, in execute_and_notify_query_listener
    raise ex
  File "<site-packages>/snowflake/snowpark/_internal/server_connection.py", line 450, in execute_and_notify_query_listener
    results_cursor = self._cursor.execute(query, **kwargs)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/connector/cursor.py", line 1144, in execute
    Error.errorhandler_wrapper(self.connection, self, error_class, errvalue)
  File "<site-packages>/snowflake/connector/errors.py", line 298, in errorhandler_wrapper
    handed_over = Error.hand_to_other_handler(
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<site-packages>/snowflake/connector/errors.py", line 354, in hand_to_other_handler
    cursor.errorhandler(connection, cursor, error_class, error_value)
  File "<site-packages>/snowflake/connector/errors.py", line 229, in default_errorhandler
    raise error_class(
snowflake.snowpark.exceptions.SnowparkSQLException: (1304): 01c39468-0001-40eb-0019-8557002b30fa: 090236 (42601): Stored procedure execution error: Unsupported statement type 'temporary TABLE'.
 in function SEND_SUPABASE_APPROVAL_REQUEST with handler main