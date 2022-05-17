{{config(
  materialized = 'table',
  partition_by = {
        "field": "CreatedDateTime",
        "data_type": "datetime"
        },
  post_hook = 'insert into {{this}}
              select * from {{ref("stg_customer_legacy_customer_legacy")}}'
  
)}}

select
MasterCustCD,
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
CreatedDateTime,
UpdatedTime
from
  {{ref('stg_customer_legacy_customer_product_sales')}}