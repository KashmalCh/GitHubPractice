 {{
  config(
    materialized= 'incremental',
    unique_key = 'cdp_unique_id',
    merge_update_columns = ['cdp_unique_id',
      'MasterCustCD',
      'custCd',
      'WrittenDate',
      'SalesId',
      'StoreId',
      'IsReturn',
      'PurchasePurpose',
      'VariantID',    
      'Class',    
      'ProductType',    
      'Segment',    
      'Vendor',    
      'Brand',    
      'ModelName',    
      'IsMattress',   
      'IsAdjustable',
      'Channel',
      'PurchaseChannel',
      'Sales',
      'ItemNumber',
      'UpdatedTime']
     

  )
}}

 with customer_master as
 (
  select
*
  from
  {{ ref("stg_customer_master") }}
  {%if is_incremental() %} 
    where RecordLoadDateTime > (select IFNULL(date_sub(MAX(CreatedDateTime), INTERVAL 3 day ), DATETIME(1900, 01, 01, 00, 00, 00))  from {{this}} )
  {%endif%}
 ),
  sales_written as (
  select
  *
  from
  {{ ref("stg_fact_sales_written_snapshot") }}
  {%if is_incremental() %} 
    where CreatedDateTime > (select IFNULL(date_sub(MAX(CreatedDateTime), INTERVAL 3 day ), DATETIME(1900, 01, 01, 00, 00, 00)) from {{this}} )
  {%endif%}
 ),
  product_master as (
  select
*
  from
  {{ ref("stg_product_master") }}
  {%if is_incremental() %} 
    where CreatedDate > (select IFNULL(date_sub(MAX(CreatedDateTime), INTERVAL 3 day ), DATETIME(1900, 01, 01, 00, 00, 00)) from {{this}} )
  {%endif%}
  ),
  store_master as (
  select
  *
  from
  {{ ref("stg_store_master") }}
  {%if is_incremental() %} 
    where CreatedDateTime > (select IFNULL(date_sub(MAX(CreatedDateTime), INTERVAL 3 day ), DATETIME(1900, 01, 01, 00, 00, 00)) from {{this}} )
  {%endif%}
  ),  
 De_dup as (
select  
MasterCustCD,
custCd,
CustomerId,
WrittenDate,
SalesId,
a.StoreId,
Sales,
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
Sales_sum , 
ItemNumber,
Current_datetime("America/Chicago") AS CreatedDateTime,
b.RecordUpdateDatetime as UpdatedTime,
row_number() OVER(partition by 	custCd,VariantID,SalesId   ORDER BY WrittenDate) as unique_id,
from  
 sales_written a 
inner join  
 customer_master b 
on a.CustomerId=b.custCd
inner join 
 product_master c 
on a.ProductId=c.VariantID
inner join 
 store_master d 
on a.StoreId=d.StoreID
 )
select 
{{dbt_utils.surrogate_key(['custCd', 'VariantID','SalesId'])}} as cdp_unique_id,
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
Sales_sum  as Sales ,
ItemNumber,
CreatedDateTime,
UpdatedTime
from
 De_dup 
where unique_id=1

