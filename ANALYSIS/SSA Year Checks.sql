with VOTER_BASE as (
    select
        FIRST_NAME AS FIRST_NAME_CLEAN,
        BIRTH_YEAR,
        2026 - BIRTH_YEAR as ACTUAL_AGE,
        case
            when BIRTH_YEAR is null then null
            when 2026 - BIRTH_YEAR between 0 and 17 then '0_17'
            when 2026 - BIRTH_YEAR between 18 and 24 then '18_24'
            when 2026 - BIRTH_YEAR between 25 and 34 then '25_34'
            when 2026 - BIRTH_YEAR between 35 and 44 then '35_44'
            when 2026 - BIRTH_YEAR between 45 and 54 then '45_54'
            when 2026 - BIRTH_YEAR between 55 and 64 then '55_64'
            when 2026 - BIRTH_YEAR >= 65 then '65_PLUS'
            else null
        end as ACTUAL_AGE_BUCKET
    from DATA_PROVIDER_VOTER.STAGE.STATE_WA_VOTER_REGISTRATION
    where BIRTH_YEAR is not null
),
EVAL as (
    select
        V.FIRST_NAME_CLEAN,
        V.BIRTH_YEAR,
        V.ACTUAL_AGE,
        V.ACTUAL_AGE_BUCKET,
        S.PREDICTED_AGE_BUCKET,
        S.CONFIDENCE_LEVEL,
        S.MAX_PROBABILITY,
        case
            when S.FIRST_NAME is not null then 1
            else 0
        end as SSA_AGE_LOOKUP_MATCH_FLAG,
        case
            when V.ACTUAL_AGE_BUCKET = S.PREDICTED_AGE_BUCKET then 1
            else 0
        end as IS_CORRECT
    from VOTER_BASE V
    left join DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_AGE_BUCKET_SPECIFIC_LOOKUP S
        on V.FIRST_NAME_CLEAN = S.FIRST_NAME
    where V.ACTUAL_AGE_BUCKET is not null
)
select CONFIDENCE_LEVEL, COUNT(*) AS TOTAL_RECORDS, COUNT_IF(IS_CORRECT = 1) AS CORRECT_RECORDS, CORRECT_RECORDS/TOTAL_RECORDS AS PERCENT_ACCURACY
from EVAL
GROUP BY CONFIDENCE_LEVEL;






--Generic
with VOTER_BASE as (
    select
        FIRST_NAME as FIRST_NAME_CLEAN,
        BIRTH_YEAR,
        2026 - BIRTH_YEAR as ACTUAL_AGE,
        case
            when BIRTH_YEAR is null then null
            when 2026 - BIRTH_YEAR between 0 and 24 then '0_24'
            when 2026 - BIRTH_YEAR between 25 and 44 then '25_44'
            when 2026 - BIRTH_YEAR between 45 and 64 then '45_64'
            when 2026 - BIRTH_YEAR >= 65 then '65_PLUS'
            else null
        end as ACTUAL_AGE_BUCKET
    from DATA_PROVIDER_VOTER.STAGE.STATE_WA_VOTER_REGISTRATION
    where BIRTH_YEAR is not null
),
EVAL as (
    select
        V.FIRST_NAME_CLEAN,
        V.BIRTH_YEAR,
        V.ACTUAL_AGE,
        V.ACTUAL_AGE_BUCKET,
        S.PREDICTED_AGE_BUCKET,
        S.CONFIDENCE_LEVEL,
        S.MAX_PROBABILITY,
        case
            when S.FIRST_NAME is not null then 1
            else 0
        end as SSA_AGE_LOOKUP_MATCH_FLAG,
        case
            when V.ACTUAL_AGE_BUCKET = S.PREDICTED_AGE_BUCKET then 1
            else 0
        end as IS_CORRECT
    from VOTER_BASE V
    left join DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_AGE_BUCKET_GENERIC_LOOKUP S
        on V.FIRST_NAME_CLEAN = S.FIRST_NAME
    where V.ACTUAL_AGE_BUCKET is not null
)
select
    CONFIDENCE_LEVEL,
    count(*) as TOTAL_RECORDS,
    count_if(IS_CORRECT = 1) as CORRECT_RECORDS,
    count_if(IS_CORRECT = 1) / count(*) as PERCENT_ACCURACY
from EVAL
group by CONFIDENCE_LEVEL
order by
    case CONFIDENCE_LEVEL
        when 'HIGH' then 1
        when 'MEDIUM' then 2
        when 'LOW' then 3
        when 'AMBIGUOUS' then 4
        else 5
    end;
-- select
--     floor(S.MAX_PROBABILITY * 10) / 10 as PROB_BUCKET,
--     count(*) as TOTAL,
--     count_if(V.ACTUAL_AGE_BUCKET = S.PREDICTED_AGE_BUCKET) as CORRECT,
--     CORRECT / TOTAL as ACCURACY
-- from EVAL V
-- join DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_AGE_BUCKET_SPECIFIC_LOOKUP S
--     on V.FIRST_NAME_CLEAN = S.FIRST_NAME
-- group by PROB_BUCKET
-- order by PROB_BUCKET desc;


    select
    floor(MAX_PROBABILITY * 10) / 10 as PROB_BUCKET,
    count(*) as RECORDS
from DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_AGE_BUCKET_SPECIFIC_LOOKUP
group by PROB_BUCKET
order by PROB_BUCKET desc;


select
    floor(S.MAX_PROBABILITY * 10) / 10 as PROB_BUCKET,
    count(*) as TOTAL,
    count_if(V.ACTUAL_AGE_BUCKET = S.PREDICTED_AGE_BUCKET) as CORRECT,
    CORRECT / TOTAL as ACCURACY
from EVAL V
join DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_AGE_BUCKET_GENERIC_LOOKUP S
    on V.FIRST_NAME_CLEAN = S.FIRST_NAME
group by PROB_BUCKET
order by PROB_BUCKET desc;