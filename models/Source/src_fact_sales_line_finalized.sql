{{ config(
  materialized='view',
	schema = 'src'
  ) }}



SELECT 
    DISTINCT SalesId
FROM {{source('dev_mfrm_sales', 'fact_sales_line_finalized')}}