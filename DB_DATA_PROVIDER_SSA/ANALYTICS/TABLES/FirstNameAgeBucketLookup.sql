create or replace dynamic table DATA_PROVIDER_SSA.ANALYTICS.FIRST_NAME_AGE_BUCKET_LOOKUP
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
        REFERENCE_YEAR - YEAR as ESTIMATED_AGE
    from DATA_PROVIDER_SSA.STAGE.FIRST_NAME_GENDER_QUANTITY_YEAR
    where FIRST_NAME is not null
      and YEAR is not null
      and QUANTITY is not null
),
AGG as (
    select
        FIRST_NAME,
        MAX(REFERENCE_YEAR) AS REFERENCE_YEAR,
        sum(case when ESTIMATED_AGE between 0 and 17 then QUANTITY else 0 end) as AGE_0_17_QTY,
        sum(case when ESTIMATED_AGE between 18 and 24 then QUANTITY else 0 end) as AGE_18_24_QTY,
        sum(case when ESTIMATED_AGE between 25 and 34 then QUANTITY else 0 end) as AGE_25_34_QTY,
        sum(case when ESTIMATED_AGE between 35 and 44 then QUANTITY else 0 end) as AGE_35_44_QTY,
        sum(case when ESTIMATED_AGE between 45 and 54 then QUANTITY else 0 end) as AGE_45_54_QTY,
        sum(case when ESTIMATED_AGE between 55 and 64 then QUANTITY else 0 end) as AGE_55_64_QTY,
        sum(case when ESTIMATED_AGE >= 65 then QUANTITY else 0 end) as AGE_65_PLUS_QTY

    from BASE
    group by FIRST_NAME
)
select
    FIRST_NAME,
    REFERENCE_YEAR,
    AGE_0_17_QTY,
    AGE_18_24_QTY,
    AGE_25_34_QTY,
    AGE_35_44_QTY,
    AGE_45_54_QTY,
    AGE_55_64_QTY,
    AGE_65_PLUS_QTY,

    AGE_0_17_QTY
    + AGE_18_24_QTY
    + AGE_25_34_QTY
    + AGE_35_44_QTY
    + AGE_45_54_QTY
    + AGE_55_64_QTY
    + AGE_65_PLUS_QTY as TOTAL_QTY,

    AGE_0_17_QTY / nullif(
        AGE_0_17_QTY
        + AGE_18_24_QTY
        + AGE_25_34_QTY
        + AGE_35_44_QTY
        + AGE_45_54_QTY
        + AGE_55_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_0_17_PROBABILITY,

    AGE_18_24_QTY / nullif(
        AGE_0_17_QTY
        + AGE_18_24_QTY
        + AGE_25_34_QTY
        + AGE_35_44_QTY
        + AGE_45_54_QTY
        + AGE_55_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_18_24_PROBABILITY,

    AGE_25_34_QTY / nullif(
        AGE_0_17_QTY
        + AGE_18_24_QTY
        + AGE_25_34_QTY
        + AGE_35_44_QTY
        + AGE_45_54_QTY
        + AGE_55_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_25_34_PROBABILITY,

    AGE_35_44_QTY / nullif(
        AGE_0_17_QTY
        + AGE_18_24_QTY
        + AGE_25_34_QTY
        + AGE_35_44_QTY
        + AGE_45_54_QTY
        + AGE_55_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_35_44_PROBABILITY,

    AGE_45_54_QTY / nullif(
        AGE_0_17_QTY
        + AGE_18_24_QTY
        + AGE_25_34_QTY
        + AGE_35_44_QTY
        + AGE_45_54_QTY
        + AGE_55_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_45_54_PROBABILITY,

    AGE_55_64_QTY / nullif(
        AGE_0_17_QTY
        + AGE_18_24_QTY
        + AGE_25_34_QTY
        + AGE_35_44_QTY
        + AGE_45_54_QTY
        + AGE_55_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_55_64_PROBABILITY,

    AGE_65_PLUS_QTY / nullif(
        AGE_0_17_QTY
        + AGE_18_24_QTY
        + AGE_25_34_QTY
        + AGE_35_44_QTY
        + AGE_45_54_QTY
        + AGE_55_64_QTY
        + AGE_65_PLUS_QTY, 0
    ) as AGE_65_PLUS_PROBABILITY,

    greatest(
        AGE_0_17_QTY / nullif(
            AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_18_24_QTY / nullif(
            AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_25_34_QTY / nullif(
            AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_35_44_QTY / nullif(
            AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_45_54_QTY / nullif(
            AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_55_64_QTY / nullif(
            AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0
        ),
        AGE_65_PLUS_QTY / nullif(
            AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0
        )
    ) as MAX_PROBABILITY,

    case
        when AGE_0_17_QTY >= AGE_18_24_QTY
         and AGE_0_17_QTY >= AGE_25_34_QTY
         and AGE_0_17_QTY >= AGE_35_44_QTY
         and AGE_0_17_QTY >= AGE_45_54_QTY
         and AGE_0_17_QTY >= AGE_55_64_QTY
         and AGE_0_17_QTY >= AGE_65_PLUS_QTY then '0_17'
        when AGE_18_24_QTY >= AGE_25_34_QTY
         and AGE_18_24_QTY >= AGE_35_44_QTY
         and AGE_18_24_QTY >= AGE_45_54_QTY
         and AGE_18_24_QTY >= AGE_55_64_QTY
         and AGE_18_24_QTY >= AGE_65_PLUS_QTY then '18_24'
        when AGE_25_34_QTY >= AGE_35_44_QTY
         and AGE_25_34_QTY >= AGE_45_54_QTY
         and AGE_25_34_QTY >= AGE_55_64_QTY
         and AGE_25_34_QTY >= AGE_65_PLUS_QTY then '25_34'
        when AGE_35_44_QTY >= AGE_45_54_QTY
         and AGE_35_44_QTY >= AGE_55_64_QTY
         and AGE_35_44_QTY >= AGE_65_PLUS_QTY then '35_44'
        when AGE_45_54_QTY >= AGE_55_64_QTY
         and AGE_45_54_QTY >= AGE_65_PLUS_QTY then '45_54'
        when AGE_55_64_QTY >= AGE_65_PLUS_QTY then '55_64'
        else '65_PLUS'
    end as PREDICTED_AGE_BUCKET,

    case
        when greatest(
            AGE_0_17_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_18_24_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_25_34_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_35_44_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_45_54_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_55_64_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_65_PLUS_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0)
        ) >= 0.90 then 'HIGH'
        when greatest(
            AGE_0_17_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_18_24_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_25_34_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_35_44_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_45_54_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_55_64_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_65_PLUS_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0)
        ) >= 0.75 then 'MEDIUM'
        when greatest(
            AGE_0_17_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_18_24_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_25_34_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_35_44_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_45_54_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_55_64_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0),
            AGE_65_PLUS_QTY / nullif(AGE_0_17_QTY + AGE_18_24_QTY + AGE_25_34_QTY + AGE_35_44_QTY + AGE_45_54_QTY + AGE_55_64_QTY + AGE_65_PLUS_QTY, 0)
        ) >= 0.60 then 'LOW'
        else 'AMBIGUOUS'
    end as CONFIDENCE_LEVEL,

    case
        when (
            AGE_0_17_QTY
            + AGE_18_24_QTY
            + AGE_25_34_QTY
            + AGE_35_44_QTY
            + AGE_45_54_QTY
            + AGE_55_64_QTY
            + AGE_65_PLUS_QTY
        ) < 100 then 1
        else 0
    end as IS_RARE_NAME

from AGG;