{{ config(
  materialized='incremental',
  schema='stg'
) }}

select
  distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(VisitorEmail) as Email,
  VisitorFirstName as FirstName,
  VisitorLastName as LastName,
  '' as Address,
  '' as City,
  '' as State,
  'LiveChat' as TableName,
  ModifiedDateTime
from
  {{ ref( "src_fact_sf_live_chat_transcript") }}
--   `mattress-firm-inc.mfrm_customer_360.fact_sf_live_chat_transcript`
where
  CustomerId = '-1'
  and VisitorEmail IS NOT NULL
  and VisitorEmail LIKE '%@%'
{% if is_incremental() %}
  and DATETIME(ModifiedDateTime) > COALESCE((select max(ModifiedDateTime) from {{this}}),'1900-01-01T00:00:00.000001')
{% endif %}
