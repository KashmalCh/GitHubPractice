{{ config(
   materialized='incremental',
  schema='stg'
) }}

select
  distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(SubscriberKey) AS Email,
  '' as FirstName,
  '' as LastName,
  '' as Address,
  '' as City,
  '' as State,
  'SFMCSentHistory' AS TableName,
  ModifiedDateTime
from
  {{ ref("src_fact_sfmc_sent_history") }}
where
  CustomerId = '-1'
  and SubscriberKey IS NOT NULL
  and SubscriberKey LIKE '%@%'
{% if is_incremental() %}
  and DATETIME(ModifiedDateTime) > COALESCE((select max(ModifiedDateTime) from {{this}}),'1900-01-01T00:00:00.000001')
{% endif %}
