{{ config(
  materialized='incremental',
  schema='stg'
) }}

select
  distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(EmailAddress) AS Email,
  '' as FirstName,
  '' as LastName,
  '' as Address,
  '' as City,
  '' as State,
  'PodiumConversations' as TableName,
  ModifiedDateTime
from  {{ ref("src_fact_podium_messages_and_conversations") }}
--   `mattress-firm-inc.mfrm_customer_360.fact_podium_messages_and_conversations`
where
  CustomerId = '-1'
  and EmailAddress IS NOT NULL
  and EmailAddress LIKE '%@%'
{% if is_incremental() %}
   and DATETIME(ModifiedDateTime) > COALESCE((select max(ModifiedDateTime) from {{this}}),'1900-01-01T00:00:00.000001')
{% endif %}
