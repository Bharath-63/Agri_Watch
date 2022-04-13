{{ config (
materialized="table"
)}}
with __dbt__cte__FOODS_FOODNUTRIENTS_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".AGRI."FOODS"

select
    _AIRBYTE_FOODS_HASHID,
    to_varchar(get_path(parse_json(FOODNUTRIENTS.value), '"id"')) as ID,
    to_varchar(get_path(parse_json(FOODNUTRIENTS.value), '"type"')) as TYPE,
    to_varchar(get_path(parse_json(FOODNUTRIENTS.value), '"amount"')) as AMOUNT,

        get_path(parse_json(FOODNUTRIENTS.value), '"nutrient"')
     as NUTRIENT,

        get_path(parse_json(FOODNUTRIENTS.value), '"foodNutrientDerivation"')
     as FOODNUTRIENTDERIVATION,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".AGRI."FOODS" as table_alias
-- FOODNUTRIENTS at foods/foodNutrients
cross join table(flatten(FOODNUTRIENTS)) as FOODNUTRIENTS
where 1 = 1
and FOODNUTRIENTS is not null

),  __dbt__cte__FOODS_FOODNUTRIENTS_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_AB1
select
    _AIRBYTE_FOODS_HASHID,
    cast(ID as
    bigint
) as ID,
    cast(TYPE as
    varchar
) as TYPE,
    cast(AMOUNT as
    float
) as AMOUNT,
    cast(NUTRIENT as
    variant
) as NUTRIENT,
    cast(FOODNUTRIENTDERIVATION as
    variant
) as FOODNUTRIENTDERIVATION,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__FOODS_FOODNUTRIENTS_AB1
-- FOODNUTRIENTS at foods/foodNutrients
where 1 = 1

),  __dbt__cte__FOODS_FOODNUTRIENTS_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_FOODS_HASHID as
    varchar
), '') || '-' || coalesce(cast(ID as
    varchar
), '') || '-' || coalesce(cast(TYPE as
    varchar
), '') || '-' || coalesce(cast(AMOUNT as
    varchar
), '') || '-' || coalesce(cast(NUTRIENT as
    varchar
), '') || '-' || coalesce(cast(FOODNUTRIENTDERIVATION as
    varchar
), '') as
    varchar
)) as _AIRBYTE_FOODNUTRIENTS_HASHID,
    tmp.*
from __dbt__cte__FOODS_FOODNUTRIENTS_AB2 tmp
-- FOODNUTRIENTS at foods/foodNutrients
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_AB3
select
    _AIRBYTE_FOODS_HASHID,
    ID,
    TYPE,
    AMOUNT,
    NUTRIENT,
    FOODNUTRIENTDERIVATION,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_FOODNUTRIENTS_HASHID
from __dbt__cte__FOODS_FOODNUTRIENTS_AB3
-- FOODNUTRIENTS at foods/foodNutrients from "DB".AGRI."FOODS"
where 1 = 1
