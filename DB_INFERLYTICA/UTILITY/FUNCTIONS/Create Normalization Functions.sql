create or replace function  INFERLYTICA.UTILITY.NORMALIZE_NAME(INPUT_VALUE varchar)
returns varchar
as
$$
    nullif(
        regexp_replace(lower(trim(INPUT_VALUE)), '[^[:alpha:]]', ''),
        ''
    )
$$;

create or replace function INFERLYTICA.UTILITY.NORMALIZE_ZIP5(INPUT_VALUE varchar)
returns varchar
as
$$
    case
        when INPUT_VALUE is null then null
        when length(regexp_replace(trim(INPUT_VALUE), '[^0-9]', '')) >= 5 then
            substr(regexp_replace(trim(INPUT_VALUE), '[^0-9]', ''), 1, 5)
        else null
    end
$$;

create or replace function INFERLYTICA.UTILITY.NORMALIZE_GENDER(INPUT_VALUE varchar)
returns varchar
as
$$
    case
        when INPUT_VALUE is null then null
        when upper(trim(INPUT_VALUE)) = 'M' OR upper(trim(INPUT_VALUE)) = 'MALE' then 'M'
        when upper(trim(INPUT_VALUE)) = 'F' OR  upper(trim(INPUT_VALUE)) = 'FEMALE' then 'F'
        else null
    end
$$;