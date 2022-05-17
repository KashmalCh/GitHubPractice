 {{
  config(
    materialized='view',
	schema = 'src'
    
  )
}}

select * 
from
  {{ source('dev_es_item','es_store') }}