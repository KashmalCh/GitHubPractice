{{ config(
  materialized='incremental',
  schema='stg'
) }}

select
  distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(EmailAddress) AS Email,
  FirstName as FirstName,
  LastName as LastName,
  Address as Address,
  City as City,
  State as State,
  'InmommentSurveys' AS TableName,
  ModifiedDateTime
from
  {{ ref( "src_fact_inmoment_surveys") }}
 --`mattress-firm-inc.mfrm_customer_360.fact_inmoment_surveys`
  where
  CustomerId = '-1'
  and EmailAddress IS NOT NULL
  and EmailAddress LIKE '%@%'
{% if is_incremental() %}
   and DATETIME(ModifiedDateTime) > COALESCE((select max(ModifiedDateTime) from {{this}}),'1900-01-01T00:00:00.000001')
{% endif %}
