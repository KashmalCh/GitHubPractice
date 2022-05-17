{{config(
    materialized = 'table'
)}}
with de_dup as (
select distinct
   COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(B.CustomerID,C.CustomerId),D.CustomerId),E.CustomerId),F.CustomerId),'-1') as CustomerId
   ,COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(B.Name,C.Name),D.Name),E.Name),F.Name),A.Name) as Name
   ,A.UnSubscriberKey as SubscriberKey
   ,A.UnsubscribeDate
   ,COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(B.EmailAddr,C.EmailAddr),D.EmailAddr),E.EmailAddr),F.EmailAddr),A.Email) as Email
   ,ROW_NUMBER() OVER(PARTITION BY COALESCE(COALESCE(COALESCE(COALESCE(COALESCE(B.EmailAddr,C.EmailAddr),D.EmailAddr),E.EmailAddr),F.EmailAddr),A.Email)  order by A.UnsubscribeDate desc) as Rc
   ,CURRENT_DATETIME("America/Chicago") as CreatedDateTime
   ,CURRENT_DATETIME("America/Chicago") as ModifiedDateTime
from
    (
select distinct
  `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(A.SubscriberKey) as UnSubscriberKey
  ,`mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(B.SubscriberKey) SubscriberKey
  ,A.UnsubscribeDate
  ,`mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(B.EmailAddress) Email
  ,B.SubscriberID
  ,B.CustomerCode
  ,CONCAT(TRIM(UPPER(B.FirstName)),' ',TRIM(UPPER(B.LastName))) as Name
from {{ref( 'src_sfmc_unsubscribers')}} A
left join {{ref( 'src_sfmc_all_subscribers')}} B
on A.SubscriberKey = B.SubscriberKey
    ) A 
left join
  {{ ref('stg_unsubscribed_customers_customer_master_emails') }}  B 
on
  A.UnSubscriberKey = B.EmailAddr
left join
  {{ ref('stg_unsubscribed_customers_customer_master_emails') }} C 
on
  A.Email = C.EmailAddr
left join
  {{ ref('stg_unsubscribed_customers_customer_master_custcodes') }} D 
on
  A.CustomerCode = D.CustomerId
left join
  {{ ref('stg_unsubscribed_customers_customer_master_sfcontactids') }} E 
on
  A.SubscriberKey = E.SFContactID
left join
  {{ ref('stg_unsubscribed_customers_customer_master_sfcontactids') }} F 
on
  A.SubscriberId = F.SFContactID
)
select 
  CustomerId
  ,A.Name
  ,SubscriberKey
  ,UnsubscribeDate
  ,Email
  ,CreatedDateTime
  ,ModifiedDateTime
from 
  de_dup A
where 
  Rc=1
    