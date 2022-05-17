 {{
  config(
    materialized='view',
	schema = 'src'
  )
}}

select	
* 
from  
    {{source('mfrm_reporting_dataset_dynamic','rpt_ga_post_purchase_sessions')}}
