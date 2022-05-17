{{ config(
  materialized='incremental',
  schema='stg'
) }}

select
distinct `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(als.EmailAddress) AS Email,
als.FirstName,
als.LastName,
als.Address,
als.City,
als.State,
'SFMC_All_Subscriber' AS TableName,
ModifiedDateTime
from
  {{ ref("src_sfmc_all_subscribers") }} als
LEFT JOIN
  {{ ref( "src_customer_master") }} cm
on
  LOWER(als.EmailAddress) = LOWER(cm.EmailAddr)
where
  (als.EmailAddress IS NOT NULL
  AND als.EmailAddress != '')
  AND cm.EmailAddr IS NULL
  AND (als.CustomerCode IS NULL
  OR als.CustomerCode !='')
  AND als.SubscriberKey NOT LIKE '%@%'
{% if is_incremental() %}
  and DATETIME(ModifiedDateTime) > COALESCE((select max(ModifiedDateTime) from {{this}}),'1900-01-01T00:00:00.000001')
{% endif %}
