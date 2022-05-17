
{{ config(
  materialized='view',
	schema = 'src'
  ) }}



SELECT 
    *
FROM
{{source('dev_mfrm_sales', 'dim_calendar')}}