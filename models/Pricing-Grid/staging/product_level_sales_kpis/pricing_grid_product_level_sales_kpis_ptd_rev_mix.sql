{{config(
    materialized = 'table',
    schema = 'stg'
)}}
SELECT DISTINCT
    CONCAT(CAST(ROUND((CASE 
        WHEN 
            COALESCE(TotalSum,0) = 0 
        THEN 
            0 
        ELSE 
            (SUM(COALESCE(SnWrSales,0)) OVER(PARTITION BY ProductId)/COALESCE(TotalSum,0))*100
    END),2) AS STRING),'%') AS RevMix
    ,ProductId
    ,SUM(COALESCE(SnWrSales,0)) OVER(PARTITION BY ProductId) AS ProductLevelSales
    ,COALESCE(TotalSum,0) TotalSales
FROM
    {{ref('pricing_grid_sales_update')}} A
CROSS JOIN 
    (
        SELECT 
            SUM(SnWrSales) TotalSum
        FROM 
            {{ref('pricing_grid_sales_update')}}
        WHERE 
            FYear = (SELECT FYearNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
            AND FPeriod = (SELECT FPeriodNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
    ) B
WHERE 
    FYear = (SELECT FYearNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
    AND FPeriod = (SELECT FPeriodNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())

