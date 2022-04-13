{{ config (
materialized="table"
)}}
with __dbt__cte__FOODS_LABELNUTRIENTS_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".AGRI."FOODS"
select
    _AIRBYTE_FOODS_HASHID,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"fat"')
     as FAT,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"iron"')
     as IRON,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"fiber"')
     as FIBER,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"sodium"')
     as SODIUM,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"sugars"')
     as SUGARS,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"calcium"')
     as CALCIUM,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"protein"')
     as PROTEIN,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"calories"')
     as CALORIES,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"transFat"')
     as TRANSFAT,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"potassium"')
     as POTASSIUM,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"cholesterol"')
     as CHOLESTEROL,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"saturatedFat"')
     as SATURATEDFAT,

        get_path(parse_json(table_alias.LABELNUTRIENTS), '"carbohydrates"')
     as CARBOHYDRATES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".AGRI."FOODS" as table_alias
-- LABELNUTRIENTS at foods/labelNutrients
where 1 = 1
and LABELNUTRIENTS is not null

),  __dbt__cte__FOODS_LABELNUTRIENTS_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__FOODS_LABELNUTRIENTS_AB1
select
    _AIRBYTE_FOODS_HASHID,
    cast(FAT as
    variant
) as FAT,
    cast(IRON as
    variant
) as IRON,
    cast(FIBER as
    variant
) as FIBER,
    cast(SODIUM as
    variant
) as SODIUM,
    cast(SUGARS as
    variant
) as SUGARS,
    cast(CALCIUM as
    variant
) as CALCIUM,
    cast(PROTEIN as
    variant
) as PROTEIN,
    cast(CALORIES as
    variant
) as CALORIES,
    cast(TRANSFAT as
    variant
) as TRANSFAT,
    cast(POTASSIUM as
    variant
) as POTASSIUM,
    cast(CHOLESTEROL as
    variant
) as CHOLESTEROL,
    cast(SATURATEDFAT as
    variant
) as SATURATEDFAT,
    cast(CARBOHYDRATES as
    variant
) as CARBOHYDRATES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__FOODS_LABELNUTRIENTS_AB1
-- LABELNUTRIENTS at foods/labelNutrients
where 1 = 1

),  __dbt__cte__FOODS_LABELNUTRIENTS_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__FOODS_LABELNUTRIENTS_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_FOODS_HASHID as
    varchar
), '') || '-' || coalesce(cast(FAT as
    varchar
), '') || '-' || coalesce(cast(IRON as
    varchar
), '') || '-' || coalesce(cast(FIBER as
    varchar
), '') || '-' || coalesce(cast(SODIUM as
    varchar
), '') || '-' || coalesce(cast(SUGARS as
    varchar
), '') || '-' || coalesce(cast(CALCIUM as
    varchar
), '') || '-' || coalesce(cast(PROTEIN as
    varchar
), '') || '-' || coalesce(cast(CALORIES as
    varchar
), '') || '-' || coalesce(cast(TRANSFAT as
    varchar
), '') || '-' || coalesce(cast(POTASSIUM as
    varchar
), '') || '-' || coalesce(cast(CHOLESTEROL as
    varchar
), '') || '-' || coalesce(cast(SATURATEDFAT as
    varchar
), '') || '-' || coalesce(cast(CARBOHYDRATES as
    varchar
), '') as
    varchar
)) as _AIRBYTE_LABELNUTRIENTS_HASHID,
    tmp.*
from __dbt__cte__FOODS_LABELNUTRIENTS_AB2 tmp
-- LABELNUTRIENTS at foods/labelNutrients
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__FOODS_LABELNUTRIENTS_AB3
select
    _AIRBYTE_FOODS_HASHID,
    FAT,
    IRON,
    FIBER,
    SODIUM,
    SUGARS,
    CALCIUM,
    PROTEIN,
    CALORIES,
    TRANSFAT,
    POTASSIUM,
    CHOLESTEROL,
    SATURATEDFAT,
    CARBOHYDRATES,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_LABELNUTRIENTS_HASHID
from __dbt__cte__FOODS_LABELNUTRIENTS_AB3
-- LABELNUTRIENTS at foods/labelNutrients from "DB".AGRI."FOODS"
where 1 = 1
