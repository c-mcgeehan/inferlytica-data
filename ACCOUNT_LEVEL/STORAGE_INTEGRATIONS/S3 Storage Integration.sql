--For client and data provider data
CREATE OR REPLACE STORAGE INTEGRATION S3_INGEST_DATA_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::151562994023:role/snowflake_s3_ingest_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/', 's3://inferlytica-snowflake-provider-data-151562994023-us-east-1-an/uploads');


  DESC INTEGRATION S3_INGEST_DATA_INTEGRATION;
  --Update S3 IAM Role:
  --STORAGE_AWS_IAM_USER_ARN	String	arn:aws:iam::004878718171:user/64ll1000-s
  --STORAGE_AWS_EXTERNAL_ID	String	NVC23198_SFCRole=3_IjbJ/7NsEouUb0Sd7waRZNemO+k=


SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
  'S3_INGEST_DATA_INTEGRATION',
  's3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/',
  'validate_list.txt',
  'list'
);

SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
  'S3_INGEST_DATA_INTEGRATION',
  's3://inferlytica-snowflake-provider-data-151562994023-us-east-1-an/uploads/',
  'validate_list.txt',
  'list'
);

SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION(
  'S3_INGEST_DATA_INTEGRATION',
  's3://inferlytica-snowflake-client-data-151562994023-us-east-1-an/uploads/',
  'updated_sample.csv',
  'read'
);