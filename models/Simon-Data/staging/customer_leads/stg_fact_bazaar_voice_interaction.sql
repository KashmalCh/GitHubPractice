{{ config(
  materialized='incremental',
  schema='stg'
) }}
{% set LoadStartDate = '1900-01-01T00:00:00.000001'  %}

select
  distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(AuthorEmailAddress) AS Email,
  AuthorName as FirstName,
  '' as LastName,
  '' as Address,
  '' as City,
  '' as State,
  'BazarVoice' AS TableName,
  ModifiedDateTime
from
  {{ ref("src_fact_bazaar_voice_interaction") }} -- `mattress-firm-inc.mfrm_customer_360.fact_bazaar_voice_interaction`
where
  CustomerId = '-1'
  and AuthorEmailAddress IS NOT NULL
  and AuthorEmailAddress LIKE '%@%'

{% if is_incremental() %}
  AND DATETIME(ModifiedDateTime) > COALESCE((select MAX(ModifiedDateTime) from {{this}}), '1900-01-01T00:00:00.000001') 
{% endif %}
