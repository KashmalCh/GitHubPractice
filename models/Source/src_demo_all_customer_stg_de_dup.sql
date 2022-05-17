{{
  config(
    materialized='view',
	schema = 'src'
  )
}}




	select	* from  
	  {{ source('demo_all_customer_stg_de_dup', 'demo_all_customer_stg_de_dup') }} 
