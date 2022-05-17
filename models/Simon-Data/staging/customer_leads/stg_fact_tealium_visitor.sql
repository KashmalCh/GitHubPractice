
{{ config(
   materialized='incremental',
  schema='stg'
) }}

select
  distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(Email) as Email,
  '' as FirstName,
  '' as LastName,
  '' as Address,
  '' as City,
  '' as State,
  'TealiumVisitor' as TableName,
  ModifiedDateTime
from {{ ref("src_fact_tealium_visitor") }}
where
  CustomerId = '-1'
  and Email IS NOT NULL
  and Email LIKE '%@%'
  -- AND TIMESTAMP(ModifiedDateTime) > LoadStartDate
{% if is_incremental() %}
  and DATETIME(ModifiedDateTime) > COALESCE((select max(ModifiedDateTime) from {{this}}),'1900-01-01T00:00:00.000001')
{% endif %}
