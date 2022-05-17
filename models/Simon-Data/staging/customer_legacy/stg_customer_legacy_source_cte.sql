{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select
distinct StoreID, 
PurchaseChannel, 
CreatedDateTime, 
WrittenDate, 
SalesID, 
cm.MasterCustCD as MasterCustCD, 
ItmCd ProductId, 
Sales, 
IsReturn, 
VariantID, 
Class,
ProductType, 
Segment, 
Vendor,
sl.Brand, 
ModelName, 
IsMattress, 
IsAdjustable,
Channel,
CreatedDate,
cCreatedDateTime,
cUpdatedTime
from 
    {{ref('stg_customer_legacy_sales_header')}} sh
join 
    {{ref('stg_customer_legacy_sales_line')}} sl
    on 
        sl.DelDocNum = sh.DelDocNum 
join  
    {{ref('stg_customer_legacy_customer_master')}} cm 
on 
    sh.custcd =cm.CustCd