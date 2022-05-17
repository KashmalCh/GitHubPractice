{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT
    ItemNumber AS VariantId
    ,SUM(COALESCE(EcomUnitsSold,0)) AS EcomUnitsSold
    ,SUM(ProductDetailViews) AS Traffic
    ,CASE WHEN SUM(ProductDetailViews) = 0 THEN 0 ELSE SUM(COALESCE(EcomUnitsSold,0))/SUM(ProductDetailViews) END AS Conversion
FROM 
	{{source('mfrm_sales_dynamic', 'pricing_grid_product_sku_level_details')}}
WHERE 
	DetailViewDate < CURRENT_DATE()
	AND FYear = (SELECT FYearNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
GROUP BY 1
