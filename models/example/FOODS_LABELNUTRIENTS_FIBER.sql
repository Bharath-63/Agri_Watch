{{ config (
materialized="table"
)}}
with __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".AGRI."FOODS_LABELNUTRIENTS"
select
    _AIRBYTE_LABELNUTRIENTS_HASHID,
    to_varchar(get_path(parse_json(FIBER), '"value"')) as VALUE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".AGRI."FOODS_LABELNUTRIENTS" as table_alias
-- FIBER at foods/labelNutrients/fiber
where 1 = 1
and FIBER is not null

),  __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB1
select
    _AIRBYTE_LABELNUTRIENTS_HASHID,
    cast(VALUE as
    float
) as VALUE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB1
-- FIBER at foods/labelNutrients/fiber
where 1 = 1

),  __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_LABELNUTRIENTS_HASHID as
    varchar
), '') || '-' || coalesce(cast(VALUE as
    varchar
), '') as
    varchar
)) as _AIRBYTE_FIBER_HASHID,
    tmp.*
from __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB2 tmp
-- FIBER at foods/labelNutrients/fiber
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB3
select
    _AIRBYTE_LABELNUTRIENTS_HASHID,
    VALUE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_FIBER_HASHID
from __dbt__cte__FOODS_LABELNUTRIENTS_FIBER_AB3
-- FIBER at foods/labelNutrients/fiber from "DB".AGRI."FOODS_LABELNUTRIENTS"
where 1 = 1

            ) order by (_AIRBYTE_EMITTED_AT)
      );
    alter table "DB".AGRI."FOODS_LABELNUTRIENTS_FIBER" cluster by (_AIRBYTE_EMITTED_AT);