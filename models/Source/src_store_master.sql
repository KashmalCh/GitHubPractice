{{
  config(
    materialized='view',
	schema = 'src'
  )
}}
    
select	
* 
from  
{{ source('mfrm_sales', 'store_master') }} 