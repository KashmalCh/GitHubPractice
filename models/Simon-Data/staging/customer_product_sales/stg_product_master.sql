 {{
  config(
    materialized='view',
    schema='stg'

  )
}}


select
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
ItemNumber,
date(CreatedDate) as CreatedDate
  from
  {{ ref("src_product_master") }}