{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT
	(B.Traffic-A.Traffic)/(B.Traffic+A.Traffic) AS WOW
	,A.VariantId
FROM 
	{{ref('pricing_grid_product_level_sales_kpis_cweek_product_details_stg')}} A
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_lweek_product_details_stg')}} B
ON 
	A.VariantId = B.VariantId
	