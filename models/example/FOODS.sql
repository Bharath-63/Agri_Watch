{{ config (
materialized="table"
)}}
with __dbt__cte__FOODS_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".AGRI._AIRBYTE_RAW_FOODS
select
    to_varchar(get_path(parse_json(_airbyte_data), '"fdcId"')) as FDCID,
    to_varchar(get_path(parse_json(_airbyte_data), '"gtinUpc"')) as GTINUPC,
    to_varchar(get_path(parse_json(_airbyte_data), '"dataType"')) as DATATYPE,
    to_varchar(get_path(parse_json(_airbyte_data), '"foodClass"')) as FOODCLASS,
    to_varchar(get_path(parse_json(_airbyte_data), '"brandOwner"')) as BRANDOWNER,
    to_varchar(get_path(parse_json(_airbyte_data), '"dataSource"')) as DATASOURCE,
    to_varchar(get_path(parse_json(_airbyte_data), '"description"')) as DESCRIPTION,
    to_varchar(get_path(parse_json(_airbyte_data), '"ingredients"')) as INGREDIENTS,
    to_varchar(get_path(parse_json(_airbyte_data), '"servingSize"')) as SERVINGSIZE,
    get_path(parse_json(_airbyte_data), '"foodPortions"') as FOODPORTIONS,
    to_varchar(get_path(parse_json(_airbyte_data), '"modifiedDate"')) as MODIFIEDDATE,
    to_varchar(get_path(parse_json(_airbyte_data), '"availableDate"')) as AVAILABLEDATE,
    get_path(parse_json(_airbyte_data), '"foodNutrients"') as FOODNUTRIENTS,
    get_path(parse_json(_airbyte_data), '"foodUpdateLog"') as FOODUPDATELOG,
    to_varchar(get_path(parse_json(_airbyte_data), '"marketCountry"')) as MARKETCOUNTRY,
    get_path(parse_json(_airbyte_data), '"foodAttributes"') as FOODATTRIBUTES,
    get_path(parse_json(_airbyte_data), '"foodComponents"') as FOODCOMPONENTS,

        get_path(parse_json(table_alias._airbyte_data), '"labelNutrients"')
     as LABELNUTRIENTS,
    to_varchar(get_path(parse_json(_airbyte_data), '"publicationDate"')) as PUBLICATIONDATE,
    to_varchar(get_path(parse_json(_airbyte_data), '"servingSizeUnit"')) as SERVINGSIZEUNIT,
    to_varchar(get_path(parse_json(_airbyte_data), '"discontinuedDate"')) as DISCONTINUEDDATE,
    to_varchar(get_path(parse_json(_airbyte_data), '"brandedFoodCategory"')) as BRANDEDFOODCATEGORY,
    to_varchar(get_path(parse_json(_airbyte_data), '"householdServingFullText"')) as HOUSEHOLDSERVINGFULLTEXT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".AGRI._AIRBYTE_RAW_FOODS as table_alias
-- FOODS
where 1 = 1

),  __dbt__cte__FOODS_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__FOODS_AB1
select
    cast(FDCID as
    bigint
) as FDCID,
    cast(GTINUPC as
    varchar
) as GTINUPC,
    cast(DATATYPE as
    varchar
) as DATATYPE,
    cast(FOODCLASS as
    varchar
) as FOODCLASS,
    cast(BRANDOWNER as
    varchar
) as BRANDOWNER,
    cast(DATASOURCE as
    varchar
) as DATASOURCE,
    cast(DESCRIPTION as
    varchar
) as DESCRIPTION,
    cast(INGREDIENTS as
    varchar
) as INGREDIENTS,
    cast(SERVINGSIZE as
    float
) as SERVINGSIZE,
    FOODPORTIONS,
    cast(MODIFIEDDATE as
    varchar
) as MODIFIEDDATE,
    cast(AVAILABLEDATE as
    varchar
) as AVAILABLEDATE,
    FOODNUTRIENTS,
    FOODUPDATELOG,
    cast(MARKETCOUNTRY as
    varchar
) as MARKETCOUNTRY,
    FOODATTRIBUTES,
    FOODCOMPONENTS,
    cast(LABELNUTRIENTS as
    variant
) as LABELNUTRIENTS,
    cast(PUBLICATIONDATE as
    varchar
) as PUBLICATIONDATE,
    cast(SERVINGSIZEUNIT as
    varchar
) as SERVINGSIZEUNIT,
    cast(DISCONTINUEDDATE as
    varchar
) as DISCONTINUEDDATE,
    cast(BRANDEDFOODCATEGORY as
    varchar
) as BRANDEDFOODCATEGORY,
    cast(HOUSEHOLDSERVINGFULLTEXT as
    varchar
) as HOUSEHOLDSERVINGFULLTEXT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from __dbt__cte__FOODS_AB1
-- FOODS
where 1 = 1

),  __dbt__cte__FOODS_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__FOODS_AB2
select
    md5(cast(coalesce(cast(FDCID as
    varchar
), '') || '-' || coalesce(cast(GTINUPC as
    varchar
), '') || '-' || coalesce(cast(DATATYPE as
    varchar
), '') || '-' || coalesce(cast(FOODCLASS as
    varchar
), '') || '-' || coalesce(cast(BRANDOWNER as
    varchar
), '') || '-' || coalesce(cast(DATASOURCE as
    varchar
), '') || '-' || coalesce(cast(DESCRIPTION as
    varchar
), '') || '-' || coalesce(cast(INGREDIENTS as
    varchar
), '') || '-' || coalesce(cast(SERVINGSIZE as
    varchar
), '') || '-' || coalesce(cast(FOODPORTIONS as
    varchar
), '') || '-' || coalesce(cast(MODIFIEDDATE as
    varchar
), '') || '-' || coalesce(cast(AVAILABLEDATE as
    varchar
), '') || '-' || coalesce(cast(FOODNUTRIENTS as
    varchar
), '') || '-' || coalesce(cast(FOODUPDATELOG as
    varchar
), '') || '-' || coalesce(cast(MARKETCOUNTRY as
    varchar
), '') || '-' || coalesce(cast(FOODATTRIBUTES as
    varchar
), '') || '-' || coalesce(cast(FOODCOMPONENTS as
    varchar
), '') || '-' || coalesce(cast(LABELNUTRIENTS as
    varchar
), '') || '-' || coalesce(cast(PUBLICATIONDATE as
    varchar
), '') || '-' || coalesce(cast(SERVINGSIZEUNIT as
    varchar
), '') || '-' || coalesce(cast(DISCONTINUEDDATE as
    varchar
), '') || '-' || coalesce(cast(BRANDEDFOODCATEGORY as
    varchar
), '') || '-' || coalesce(cast(HOUSEHOLDSERVINGFULLTEXT as
    varchar
), '') as
    varchar
)) as _AIRBYTE_FOODS_HASHID,
    tmp.*
from __dbt__cte__FOODS_AB2 tmp
-- FOODS
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__FOODS_AB3
select
    FDCID,
    GTINUPC,
    DATATYPE,
    FOODCLASS,
    BRANDOWNER,
    DATASOURCE,
    DESCRIPTION,
    INGREDIENTS,
    SERVINGSIZE,
    FOODPORTIONS,
    MODIFIEDDATE,
    AVAILABLEDATE,
    FOODNUTRIENTS,
    FOODUPDATELOG,
    MARKETCOUNTRY,
    FOODATTRIBUTES,
    FOODCOMPONENTS,
    LABELNUTRIENTS,
    PUBLICATIONDATE,
    SERVINGSIZEUNIT,
    DISCONTINUEDDATE,
    BRANDEDFOODCATEGORY,
    HOUSEHOLDSERVINGFULLTEXT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_FOODS_HASHID
from __dbt__cte__FOODS_AB3
-- FOODS from "DB".AGRI._AIRBYTE_RAW_FOODS
where 1 = 1

            ) order by (_AIRBYTE_EMITTED_AT)
      );
    alter table "DB".AGRI."FOODS" cluster by (_AIRBYTE_EMITTED_AT);