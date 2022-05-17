 {{
  config(
    materialized='table'
    
  )
}}
select 
MasterCustCD,
custCd,
WrittenDate,
SalesId,
StoreId,
IsReturn,
PurchasePurpose,
VariantID,    
Class,    
ProductType,    
Segment,    
Vendor,    
Brand,    
ModelName,    
IsMattress,   
IsAdjustable,
Channel,
PurchaseChannel,
Sales,
ItemNumber,
CreatedDateTime,
UpdatedTime
from
  {{ ref("stg_customer_product_sales") }} 


