{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT DISTINCT
    A.ProductId
    ,(A.Sales-B.Sales)/(CASE WHEN (A.Sales+B.Sales)=0 THEN 1 ELSE (A.Sales+B.Sales) END) AS Pct_Sales_WOW
    ,(A.Sales_Unit-B.Sales_Unit)/(CASE WHEN (A.Sales_Unit+B.Sales_Unit)=0 THEN 1 ELSE (A.Sales_Unit+B.Sales_Unit) END) AS Pct_Sales_Unit_WOW
    ,(A.AUSP-B.AUSP)/(CASE WHEN (A.AUSP+B.AUSP)=0 THEN 1 ELSE (A.AUSP+B.AUSP) END) AS Pct_AUSP_WOW
    ,(A.TC_P_BPS_Change_LY-B.TC_P_BPS_Change_LY)/(CASE WHEN (A.TC_P_BPS_Change_LY+B.TC_P_BPS_Change_LY)=0 THEN 1 ELSE (A.TC_P_BPS_Change_LY+B.TC_P_BPS_Change_LY) END) AS TC_P_BPS_Change_LY_Wow
    ,(A.CyPPSF-B.CyPPSF)/(CASE WHEN (A.CyPPSF+B.CyPPSF) = 0 THEN 1 ELSE (A.CyPPSF+B.CyPPSF) END) AS PPSF_CH_WOW
FROM
    {{ref('pricing_grid_product_level_sales_kpis_cweek_stg')}} A
LEFT JOIN 
    {{ref('pricing_grid_product_level_sales_kpis_lweek_stg')}} B
ON 
    A.ProductId = B.ProductId
