 {{
  config(
    materialized='view',
    schema='stg'

  )
}}

with source as (  
	select	* from  
	  {{ ref( "src_fact_sales_written_snapshot") }} 
	 ),
source2 as (
select    
CONCAT('AX-',CustomerId) as CustomerId,
WrittenDate,
SalesId,
StoreId,
ProductId,
Sales,
IsReturn,
PurchasePurpose,
CreatedDateTime
from source

)
select * , 
SUM(Sales) OVER (PARTITION BY CustomerId,ProductId,SalesId  order by WrittenDate) AS Sales_sum
from source2 
