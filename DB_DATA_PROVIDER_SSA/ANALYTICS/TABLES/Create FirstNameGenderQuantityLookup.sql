create or replace dynamic table DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_GENDER_QUANTITY_LOOKUP
target_lag = DOWNSTREAM
warehouse = COMPUTE_WH
REFRESH_MODE = INCREMENTAL
INITIALIZE = ON_CREATE
as
with agg as (
    select
        FIRST_NAME,
        sum(case when GENDER = 'M' then QUANTITY else 0 end) as MALE_QTY,
        sum(case when GENDER = 'F' then QUANTITY else 0 end) as FEMALE_QTY
    from DATA_PROVIDER_SSA.STAGE.FIRST_NAME_GENDER_QUANTITY_YEAR
    group by FIRST_NAME
)
select
    first_name,
    male_qty,
    female_qty,
    male_qty + female_qty as total_qty,
    male_qty / nullif(male_qty + female_qty, 0) as male_probability,
    female_qty / nullif(male_qty + female_qty, 0) as female_probability,
    greatest(MALE_PROBABILITY, FEMALE_PROBABILITY) as MAX_PROBABILITY,
    abs(MALE_PROBABILITY - FEMALE_PROBABILITY) as PROBABILITY_GAP,
    case
    when MALE_PROBABILITY > FEMALE_PROBABILITY then 'M'
    when FEMALE_PROBABILITY > MALE_PROBABILITY then 'F'
    else 'U'
end as PREDICTED_GENDER,
case
    when greatest(MALE_PROBABILITY, FEMALE_PROBABILITY) >= 0.75
         and MALE_PROBABILITY > FEMALE_PROBABILITY then 'M'
    when greatest(MALE_PROBABILITY, FEMALE_PROBABILITY) >= 0.75
         and FEMALE_PROBABILITY > MALE_PROBABILITY then 'F'
    else 'U'
end as REPORTABLE_GENDER,
CASE
    WHEN GREATEST(MALE_PROBABILITY, FEMALE_PROBABILITY) >= 0.90 THEN 'HIGH'
    WHEN GREATEST(MALE_PROBABILITY, FEMALE_PROBABILITY) >= 0.75 THEN 'MEDIUM'
    WHEN GREATEST(MALE_PROBABILITY, FEMALE_PROBABILITY) >= 0.60 THEN 'LOW'
    ELSE 'AMBIGUOUS'
END AS CONFIDENCE_LEVEL,
case 
    when (MALE_QTY + FEMALE_QTY) < 100 then 1 else 0
end as IS_RARE_NAME
from agg;

SELECT *
FROM DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_GENDER_QUANTITY_LOOKUP;