{{ config (
materialized="table"
)}}
with __dbt__cte__FOODS_FOODUPDATELOG_AB1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "DB".AGRI."FOODS"

select
    _AIRBYTE_FOODS_HASHID,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"fdcId"')) as FDCID,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"gtinUpc"')) as GTINUPC,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"dataType"')) as DATATYPE,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"foodClass"')) as FOODCLASS,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"brandOwner"')) as BRANDOWNER,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"dataSource"')) as DATASOURCE,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"description"')) as DESCRIPTION,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"ingredients"')) as INGREDIENTS,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"servingSize"')) as SERVINGSIZE,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"modifiedDate"')) as MODIFIEDDATE,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"availableDate"')) as AVAILABLEDATE,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"marketCountry"')) as MARKETCOUNTRY,
    get_path(parse_json(FOODUPDATELOG.value), '"foodAttributes"') as FOODATTRIBUTES,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"publicationDate"')) as PUBLICATIONDATE,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"servingSizeUnit"')) as SERVINGSIZEUNIT,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"discontinuedDate"')) as DISCONTINUEDDATE,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"brandedFoodCategory"')) as BRANDEDFOODCATEGORY,
    to_varchar(get_path(parse_json(FOODUPDATELOG.value), '"householdServingFullText"')) as HOUSEHOLDSERVINGFULLTEXT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT
from "DB".AGRI."FOODS" as table_alias
-- FOODUPDATELOG at foods/foodUpdateLog
cross join table(flatten(FOODUPDATELOG)) as FOODUPDATELOG
where 1 = 1
and FOODUPDATELOG is not null

),  __dbt__cte__FOODS_FOODUPDATELOG_AB2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__FOODS_FOODUPDATELOG_AB1
select
    _AIRBYTE_FOODS_HASHID,
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
    cast(MODIFIEDDATE as
    varchar
) as MODIFIEDDATE,
    cast(AVAILABLEDATE as
    varchar
) as AVAILABLEDATE,
    cast(MARKETCOUNTRY as
    varchar
) as MARKETCOUNTRY,
    FOODATTRIBUTES,
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
from __dbt__cte__FOODS_FOODUPDATELOG_AB1
-- FOODUPDATELOG at foods/foodUpdateLog
where 1 = 1

),  __dbt__cte__FOODS_FOODUPDATELOG_AB3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__FOODS_FOODUPDATELOG_AB2
select
    md5(cast(coalesce(cast(_AIRBYTE_FOODS_HASHID as
    varchar
), '') || '-' || coalesce(cast(FDCID as
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
), '') || '-' || coalesce(cast(MODIFIEDDATE as
    varchar
), '') || '-' || coalesce(cast(AVAILABLEDATE as
    varchar
), '') || '-' || coalesce(cast(MARKETCOUNTRY as
    varchar
), '') || '-' || coalesce(cast(FOODATTRIBUTES as
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
)) as _AIRBYTE_FOODUPDATELOG_HASHID,
    tmp.*
from __dbt__cte__FOODS_FOODUPDATELOG_AB2 tmp
-- FOODUPDATELOG at foods/foodUpdateLog
where 1 = 1

)-- Final base SQL model
-- depends_on: __dbt__cte__FOODS_FOODUPDATELOG_AB3
select
    _AIRBYTE_FOODS_HASHID,
    FDCID,
    GTINUPC,
    DATATYPE,
    FOODCLASS,
    BRANDOWNER,
    DATASOURCE,
    DESCRIPTION,
    INGREDIENTS,
    SERVINGSIZE,
    MODIFIEDDATE,
    AVAILABLEDATE,
    MARKETCOUNTRY,
    FOODATTRIBUTES,
    PUBLICATIONDATE,
    SERVINGSIZEUNIT,
    DISCONTINUEDDATE,
    BRANDEDFOODCATEGORY,
    HOUSEHOLDSERVINGFULLTEXT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    convert_timezone('UTC', current_timestamp()) as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_FOODUPDATELOG_HASHID
from __dbt__cte__FOODS_FOODUPDATELOG_AB3
-- FOODUPDATELOG at foods/foodUpdateLog from "DB".AGRI."FOODS"
where 1 = 1
