{{config(
    materialized='table'
)}}

select 
 distinct CustomerId 
from  
 {{source('mfrm_sales', 'fact_sales_written_snapshot')}}