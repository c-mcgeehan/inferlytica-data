CREATE OR REPLACE TASK CUSTOMER.FILE_PROCESSING.SEND_CLIENT_BATCH_NOTIFICATIONS_TASK
  USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS = 10
  SERVERLESS_TASK_MAX_STATEMENT_SIZE = XSMALL
  TARGET_COMPLETION_INTERVAL = '15 MINUTES'
  WHEN SYSTEM$STREAM_HAS_DATA('CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS_STREAM')
AS
CALL CUSTOMER.FILE_PROCESSING.SEND_CLIENT_BATCH_NOTIFICATIONS();

ALTER TASK CUSTOMER.FILE_PROCESSING.SEND_CLIENT_BATCH_NOTIFICATIONS_TASK RESUME;
CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS_STREAM;

SELECT *
FROM TABLE(
  CUSTOMER.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'SEND_CLIENT_BATCH_NOTIFICATIONS_TASK',
    RESULT_LIMIT => 50
  )
)
ORDER BY SCHEDULED_TIME DESC;



SELECT *
FROM CUSTOMER.FILE_PROCESSING.CLIENT_BATCH_NOTIFICATIONS;

{"batch_id":"92c792ec-e1e3-4997-8344-b363e5f8d1a8","delivery_status":"READY","delivery_status_updated_at":"2026-04-06 09:39:23.539 -0700","gender_metrics":[{"metric_name":"Confidence Level","metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":1},{"label":"HIGH","value":97},{"label":"LOW","value":1},{"label":"MEDIUM","value":1}]},{"metric_name":"Missing First Name","metric_type":"COUNT","metrics":[{"label":"MISSING_FIRST_NAME","value":0}]},{"metric_name":"Average Probability Gap","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_PROBABILITY_GAP","value":0.963277}]},{"metric_name":"Gender","metric_type":"COUNT","metrics":[{"label":"F","value":48},{"label":"M","value":50},{"label":"U","value":2}]},{"metric_name":"Uncommon Names","metric_type":"COUNT","metrics":[{"label":"TOTAL_UNCOMMON_NAMES","value":0}]},{"metric_name":"Total Records","metric_type":"COUNT","metrics":[{"label":"TOTAL_RECORDS","value":100}]},{"metric_name":"Uncommon Name Confidence Level","metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":0},{"label":"HIGH","value":0},{"label":"LOW","value":0},{"label":"MEDIUM","value":0}]},{"metric_name":"Average Max Probability","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_MAX_PROBABILITY","value":0.981638}]}],"organization_id":"1d96f728-bdc7-4554-8167-89cbd072b075"}

{"batch_id":"92c792ec-e1e3-4997-8344-b363e5f8d1a8","delivery_status":"READY","delivery_status_updated_at":"2026-04-06 09:39:23.539 -0700","gender_metrics":[{"metric_name":"Confidence Level","metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":1},{"label":"HIGH","value":97},{"label":"LOW","value":1},{"label":"MEDIUM","value":1}]},{"metric_name":"Missing First Name","metric_type":"COUNT","metrics":[{"label":"MISSING_FIRST_NAME","value":0}]},{"metric_name":"Average Probability Gap","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_PROBABILITY_GAP","value":0.963277}]},{"metric_name":"Gender","metric_type":"COUNT","metrics":[{"label":"F","value":48},{"label":"M","value":50},{"label":"U","value":2}]},{"metric_name":"Uncommon Names","metric_type":"COUNT","metrics":[{"label":"TOTAL_UNCOMMON_NAMES","value":0}]},{"metric_name":"Total Records","metric_type":"COUNT","metrics":[{"label":"TOTAL_RECORDS","value":100}]},{"metric_name":"Uncommon Name Confidence Level","metric_type":"COUNT","metrics":[{"label":"AMBIGUOUS","value":0},{"label":"HIGH","value":0},{"label":"LOW","value":0},{"label":"MEDIUM","value":0}]},{"metric_name":"Average Max Probability","metric_type":"AVERAGE","metrics":[{"label":"AVERAGE_MAX_PROBABILITY","value":0.981638}]}],"organization_id":"1d96f728-bdc7-4554-8167-89cbd072b075"}

--Python Interpreter Error:
Traceback (most recent call last):
  File "_udf_code.py", line 27, in run
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
snowflake.snowpark.exceptions.SnowparkSQLException: (1304): 01c37623-0001-2fcd-0019-855700183f56: 002003 (42S02): SQL compilation error:
Object 'TMP_CLIENT_BATCH_NOTIFICATIONS' does not exist or not authorized.
 in function SEND_CLIENT_BATCH_NOTIFICATIONS with handler run