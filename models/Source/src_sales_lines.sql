 {{
  config(
    materialized='view',
	schema = 'src'
  )
}}


select * from
  {{ source('mfrm_sales', 'sales_lines') }} 
