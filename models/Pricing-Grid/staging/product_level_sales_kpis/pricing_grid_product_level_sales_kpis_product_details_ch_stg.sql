{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT
	(A.Traffic-B.Traffic)/(CASE WHEN (A.Traffic+B.Traffic)=0 THEN 1 ELSE (A.Traffic+B.Traffic) END) AS CH_LY
	,A.VariantId
FROM 
	{{ref('pricing_grid_product_level_sales_kpis_cyear_product_details_stg')}} A
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_lyear_product_details_stg')}} B
ON 
	A.VariantId = B.VariantId
	