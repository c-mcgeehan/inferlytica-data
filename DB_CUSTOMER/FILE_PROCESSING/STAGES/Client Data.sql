LIST @CUSTOMER.FILE_PROCESSING.CLIENT_DATA_STAGE PATTERN='.*updated_sample\\.csv';

CREATE OR REPLACE STAGE CUSTOMER.FILE_PROCESSING.CLIENT_DATA_STAGE
  URL = 's3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/'
  STORAGE_INTEGRATION = S3_INGEST_DATA_INTEGRATION;

  CREATE OR REPLACE STAGE CUSTOMER.FILE_PROCESSING.CLIENT_DATA_DOWNLOAD_STAGE
  URL = 's3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/downloads/'
  STORAGE_INTEGRATION = S3_INGEST_DATA_INTEGRATION;

  DESC STORAGE INTEGRATION S3_INGEST_DATA_INTEGRATION;

  ALTER STORAGE INTEGRATION S3_INGEST_DATA_INTEGRATION
   SET STORAGE_ALLOWED_LOCATIONS = ('s3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/','s3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/downloads/');

   s3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/,s3://inferlytica-snowflake-provider-data-151562994023-us-east-1-an/uploads, s3://inferlytica-snowflake-provider-data-151562994023-us-east-1-an/downloads, s3://inferlytica-snowflake-provider-data-151562994023-us-east-1-an/downloads/