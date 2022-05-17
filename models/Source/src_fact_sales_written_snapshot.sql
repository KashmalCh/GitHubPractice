
 {{
  config(
    materialized='view',
	schema = 'src'

  )
}}

select	
CustomerId,
WrittenDate,
SalesId,
IsReturn,
UnitPrice,
Qty,
Cost,
StoreId,
ProductId,
Sales,
PurchasePurpose,
CreatedDateTime 
from  
	{{ source('mfrm_sales', 'fact_sales_written_snapshot') }} 