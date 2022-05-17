{{ config(
    materialized='incremental',
  schema='stg'
) }}
  
select
  distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(Email) AS Email,
  '' as FirstName,
  '' as LastName,
  '' as Address,
  '' as City,
  '' as State,
  'PodiumReviews' as TableName,
  ModifiedDateTime
from
  {{ ref("src_fact_podium_reviews") }} 
where
  CustomerId = '-1'
  and Email IS NOT NULL
  and Email LIKE '%@%'
{% if is_incremental() %}
  AND DATETIME(ModifiedDateTime) > COALESCE((select max(ModifiedDateTime) from {{this}}),'1900-01-01T00:00:00.000001')
{% endif %}
