{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select 
CustCd,
MasterCustCD,
date(ESModifiedDatetime) as ESModifiedDatetime,
RecordLoadDateTime  as cCreatedDateTime,
RecordUpdateDatetime as cUpdatedTime,
from 

    `mattress-firm-inc.mfrm_customer_and_social.customer_master`
order by 	
ESModifiedDatetime desc
