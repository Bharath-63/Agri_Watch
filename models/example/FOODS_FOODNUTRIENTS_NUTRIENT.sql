{{ config (
materialized="table"
)}}
with __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".AGRI."FOODS_FOODNUTRIENTS"
select
    _AIRBYTE_FOODNUTRIENTS_HASHID,
    to_varchar(get_path(parse_json(NUTRIENT), '"id"')) as ID,
    to_varchar(get_path(parse_json(NUTRIENT), '"name"')) as NAME,
    to_varchar(get_path(parse_json(NUTRIENT), '"rank"')) as RANK,
    to_varchar(get_path(parse_json(NUTRIENT), '"number"')) as NUMBER,
    to_varchar(get_path(parse_json(NUTRIENT), '"unitName"')) as UNITNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".AGRI."FOODS_FOODNUTRIENTS" as table_alias
-- NUTRIENT at foods/foodNutrients/nutrient
where 1 = 1
and NUTRIENT is not null

),  __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB1
select
    _AIRBYTE_FOODNUTRIENTS_HASHID,
    cast(ID as
    bigint
) as ID,
    cast(NAME as
    varchar
) as NAME,
    cast(RANK as
    bigint
) as RANK,
    cast(NUMBER as
    varchar
) as NUMBER,
    cast(UNITNAME as
    varchar
) as UNITNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB1
-- NUTRIENT at foods/foodNutrients/nutrient
where 1 = 1

),  __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_FOODNUTRIENTS_HASHID as
    varchar
), '') || '-' || coalesce(cast(ID as
    varchar
), '') || '-' || coalesce(cast(NAME as
    varchar
), '') || '-' || coalesce(cast(RANK as
    varchar
), '') || '-' || coalesce(cast(NUMBER as
    varchar
), '') || '-' || coalesce(cast(UNITNAME as
    varchar
), '') as
    varchar
)) as _AIRBYTE_NUTRIENT_HASHID,
    tmp.*
from __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB2 tmp
-- NUTRIENT at foods/foodNutrients/nutrient
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB3
select
    _AIRBYTE_FOODNUTRIENTS_HASHID,
    ID,
    NAME,
    RANK,
    NUMBER,
    UNITNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_NUTRIENT_HASHID
from __dbt__cte__FOODS_FOODNUTRIENTS_NUTRIENT_AB3
-- NUTRIENT at foods/foodNutrients/nutrient from "DB".AGRI."FOODS_FOODNUTRIENTS"
where 1 = 1
