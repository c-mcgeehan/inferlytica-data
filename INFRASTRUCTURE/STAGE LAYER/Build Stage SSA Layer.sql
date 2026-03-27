CREATE SCHEMA DATA_PROVIDER_SSA.STAGE;

create or replace transient dynamic table DATA_PROVIDER_SSA.STAGE.FIRST_NAME_GENDER_QUANTITY_YEAR
target_lag = DOWNSTREAM
warehouse = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
INITIALIZE = ON_CREATE
as
select
    lower(trim(FIRST_NAME)) as FIRST_NAME,
    upper(trim(GENDER)) as GENDER,
    QUANTITY as QUANTITY,
    YEAR as YEAR
from DATA_PROVIDER_SSA.RAW.FIRST_NAME_GENDER_QUANTITY_YEAR
where FIRST_NAME is not null
  and 
    (
        upper(trim(GENDER)) = 'M' 
        OR
        upper(trim(GENDER)) = 'F'
    )
  and QUANTITY is not null
  and YEAR is not null;


SELECT *
FROM DATA_PROVIDER_SSA.STAGE.FIRST_NAME_GENDER_QUANTITY_YEAR;