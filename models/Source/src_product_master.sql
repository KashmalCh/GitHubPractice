 {{
  config(
    materialized='view',
	schema = 'src'
  )
}}

select	
* 
from  
    {{ source('mfrm_sales', 'product_master') }} 
