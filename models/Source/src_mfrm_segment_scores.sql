      
{{
  config(
    materialized='view',
	schema = 'src'
  )
}}


	select	* from  
	  {{ source('mfrm_segment_scores', 'mfrm_segment_scores') }} 

