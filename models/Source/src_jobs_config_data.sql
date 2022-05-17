{{ config(
  materialized='view',
	schema = 'src'
  ) }}



SELECT 
    *
FROM {{source('mfrm_pricing_grid', 'jobs_config_data')}}