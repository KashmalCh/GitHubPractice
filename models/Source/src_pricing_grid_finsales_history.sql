{{ config(
  materialized='view',
	schema = 'src'
  ) }}



SELECT 
    *
FROM {{source('mfrm_pricing_grid', 'pricing_grid_finsales_history')}}