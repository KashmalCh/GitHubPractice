{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT
    SUM(A.ForeCastPct)*SUM(B.SnWrUnits) AS FRCST_Sales
    ,SUM(A.ForeCastPct)*SUM(B.TC_D) AS FRCST_TC_D
    ,SUM(A.ForeCastPct)*SUM(B.TC_P) AS FRCST_TC_P
    ,A.ProductId
    FROM(
        SELECT
            ServicingDCNo
            ,WarehouseName
            ,OpsMarket
            ,Vendor
            ,Brand
            ,ModelName
            ,ItemNumber
            ,Size
            ,ProductID
            ,Sum
            ,ProductType
            ,Box
            ,DelMode
            ,CoreCostUnit
            ,Active
            ,MerchSalePrice
            ,CAST(ForeCastPct AS NUMERIC) ForeCastPct
            ,FYearWeek AS FYearWeekSource
            ,CAST(REPLACE(REPLACE(REPLACE(FYearWeek,'_',''),'FY','20'),'Wk','') AS INT64) AS FYearWeek
            ,CAST(SUBSTR(REPLACE(REPLACE(REPLACE(FYearWeek,'_',''),'FY','20'),'Wk',''),1,4) AS INT64) AS FYear
            ,CAST(SUBSTR(REPLACE(REPLACE(REPLACE(FYearWeek,'_',''),'FY','20'),'Wk',''),5,6) AS INT64) AS FWeek
            ,CreatedBy
            ,CreatedDateTime
            ,ModifiedBy
            ,ModifiedDateTime
        FROM {{source('mfrm_sales_dynamic', 'ops_facing_forecast')}}
        WHERE CAST(SUBSTR(REPLACE(REPLACE(REPLACE(FYearWeek,'_',''),'FY','20'),'Wk',''),1,4) AS INT64) = (SELECT FYearNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
        AND CAST(SUBSTR(REPLACE(REPLACE(REPLACE(FYearWeek,'_',''),'FY','20'),'Wk',''),5,6) AS INT64) = (SELECT FWeekNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())-1
        ) A
LEFT JOIN {{ref('pricing_grid_product_level_sales_kpis_lweek_stg')}} B
ON A.ProductId = B.ProductId
GROUP BY 4
