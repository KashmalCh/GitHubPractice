{{
  config(
    materialized='table'
  )
}}

with source as (  
	select	* from  
	  {{ source('mfrm_sales', 'store_master') }} 
	 ) 


select 
StoreID,
PurchaseChannel ,
date(CreatedDateTime) as CreatedDateTime
from source
