create or replace dynamic table DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_AGE_BUCKET_GENERIC_LOOKUP
target_lag = DOWNSTREAM
warehouse = COMPUTE_WH
refresh_mode = incremental
initialize = on_create
as
with BASE as (
    select
        2026 as REFERENCE_YEAR,
        FIRST_NAME,
        YEAR,
        QUANTITY,
        2026 - YEAR as ESTIMATED_AGE
    from DATA_PROVIDER_SSA.STAGE.FIRST_NAME_GENDER_QUANTITY_YEAR
    where FIRST_NAME is not null
      and YEAR is not null
      and QUANTITY is not null
),
AGG as (
    select
        FIRST_NAME,
        max(REFERENCE_YEAR) as REFERENCE_YEAR,
        sum(case when ESTIMATED_AGE between 0 and 24 then QUANTITY else 0 end) as AGE_0_24_QTY,
        sum(case when ESTIMATED_AGE between 25 and 44 then QUANTITY else 0 end) as AGE_25_44_QTY,
        sum(case when ESTIMATED_AGE between 45 and 64 then QUANTITY else 0 end) as AGE_45_64_QTY,
        sum(case when ESTIMATED_AGE >= 65 then QUANTITY else 0 end) as AGE_65_PLUS_QTY
    from BASE
    group by FIRST_NAME
)
select
    FIRST_NAME,
    REFERENCE_YEAR,
    AGE_0_24_QTY,
    AGE_25_44_QTY,
    AGE_45_64_QTY,
    AGE_65_PLUS_QTY,

    AGE_0_24_QTY
    + AGE_25_44_QTY
    + AGE_45_64_QTY
    + AGE_65_PLUS_QTY as TOTAL_QTY,

    AGE_0_24_QTY / nullif(
        AGE_0_24_QTY
        + AGE_25_44_QTY
        + AGE_45_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_0_24_PROBABILITY,

    AGE_25_44_QTY / nullif(
        AGE_0_24_QTY
        + AGE_25_44_QTY
        + AGE_45_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_25_44_PROBABILITY,

    AGE_45_64_QTY / nullif(
        AGE_0_24_QTY
        + AGE_25_44_QTY
        + AGE_45_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_45_64_PROBABILITY,

    AGE_65_PLUS_QTY / nullif(
        AGE_0_24_QTY
        + AGE_25_44_QTY
        + AGE_45_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_65_PLUS_PROBABILITY,

    greatest(
        AGE_0_24_QTY / nullif(
            AGE_0_24_QTY + AGE_25_44_QTY + AGE_45_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_25_44_QTY / nullif(
            AGE_0_24_QTY + AGE_25_44_QTY + AGE_45_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_45_64_QTY / nullif(
            AGE_0_24_QTY + AGE_25_44_QTY + AGE_45_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_65_PLUS_QTY / nullif(
            AGE_0_24_QTY + AGE_25_44_QTY + AGE_45_64_QTY + AGE_65_PLUS_QTY, 0
        )
    ) as MAX_PROBABILITY,

    case
        when AGE_0_24_QTY >= AGE_25_44_QTY
         and AGE_0_24_QTY >= AGE_45_64_QTY
         and AGE_0_24_QTY >= AGE_65_PLUS_QTY then '0_24'
        when AGE_25_44_QTY >= AGE_45_64_QTY
         and AGE_25_44_QTY >= AGE_65_PLUS_QTY then '25_44'
        when AGE_45_64_QTY >= AGE_65_PLUS_QTY then '45_64'
        else '65_PLUS'
    end as PREDICTED_AGE_BUCKET,

    case
        when TOTAL_QTY < 250 then 'AMBIGUOUS'
        when MAX_PROBABILITY >= 0.70 then 'HIGH'
        when MAX_PROBABILITY >= 0.55 then 'MEDIUM'
        when MAX_PROBABILITY >= 0.40 then 'LOW'
        else 'AMBIGUOUS'
    end as CONFIDENCE_LEVEL,

    case
        when (
            AGE_0_24_QTY
            + AGE_25_44_QTY
            + AGE_45_64_QTY
            + AGE_65_PLUS_QTY
        ) < 100 then 1
        else 0
    end as IS_RARE_NAME

from AGG;