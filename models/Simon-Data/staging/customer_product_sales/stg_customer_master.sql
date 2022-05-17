 {{
  config(
    materialized='view',
    schema='stg'

  )
}}


select 
CustCd,
CustCdSD,
MasterCustCD,
date(ESModifiedDatetime) as ESModifiedDatetime,
RecordUpdateDatetime,
RecordLoadDateTime
from   {{ ref("src_customer_master") }}
order by ESModifiedDatetime desc 
