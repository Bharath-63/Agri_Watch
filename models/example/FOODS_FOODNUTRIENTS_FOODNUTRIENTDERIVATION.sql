{{ config (
materialized="table"
)}}
with __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".AGRI."FOODS_FOODNUTRIENTS"
select
    _AIRBYTE_FOODNUTRIENTS_HASHID,
    to_varchar(get_path(parse_json(FOODNUTRIENTDERIVATION), '"id"')) as ID,
    to_varchar(get_path(parse_json(FOODNUTRIENTDERIVATION), '"code"')) as CODE,
    to_varchar(get_path(parse_json(FOODNUTRIENTDERIVATION), '"description"')) as DESCRIPTION,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".AGRI."FOODS_FOODNUTRIENTS" as table_alias
-- FOODNUTRIENTDERIVATION at foods/foodNutrients/foodNutrientDerivation
where 1 = 1
and FOODNUTRIENTDERIVATION is not null

),  __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB1
select
    _AIRBYTE_FOODNUTRIENTS_HASHID,
    cast(ID as
    bigint
) as ID,
    cast(CODE as
    varchar
) as CODE,
    cast(DESCRIPTION as
    varchar
) as DESCRIPTION,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB1
-- FOODNUTRIENTDERIVATION at foods/foodNutrients/foodNutrientDerivation
where 1 = 1

),  __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_FOODNUTRIENTS_HASHID as
    varchar
), '') || '-' || coalesce(cast(ID as
    varchar
), '') || '-' || coalesce(cast(CODE as
    varchar
), '') || '-' || coalesce(cast(DESCRIPTION as
    varchar
), '') as
    varchar
)) as _AIRBYTE_FOODNUTRIENTDERIVATION_HASHID,
    tmp.*
from __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB2 tmp
-- FOODNUTRIENTDERIVATION at foods/foodNutrients/foodNutrientDerivation
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB3
select
    _AIRBYTE_FOODNUTRIENTS_HASHID,
    ID,
    CODE,
    DESCRIPTION,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_FOODNUTRIENTDERIVATION_HASHID
from __dbt__cte__FOODS_FOODNUTRIENTS_FOODNUTRIENTDERIVATION_AB3
-- FOODNUTRIENTDERIVATION at foods/foodNutrients/foodNutrientDerivation from "DB".AGRI."FOODS_FOODNUTRIENTS"
where 1 = 1
