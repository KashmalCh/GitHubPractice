{{config(
    materialized = 'incremental'
    ,unique_key = "Variant_Id"
)}}

SELECT DISTINCT 
	A.*
    ,AD.Diff AS Current_Week_ATP
    ,CONCAT(CAST(ROUND(((ADL.Diff-AD.Diff)/(CASE WHEN (ADL.Diff+AD.Diff)=0 THEN 1 ELSE (ADL.Diff+AD.Diff) END))*100,2) AS STRING),'%') AS Current_Week_ATP_Pct_CH_WOW
    ,CONCAT('$',CAST(ROUND(CWF.FRCST_Sales/1000000,2) AS STRING)) AS Current_Week_FRCST_Sales
    ,CONCAT('$',CAST(ROUND(CWF.FRCST_TC_D/1000000,2) AS STRING)) AS Current_Week_FRCST_TC_D
    ,CONCAT(CAST(ROUND(CWF.FRCST_TC_P/100,2) AS STRING),'%') AS Current_Week_FRCST_TC_P
    ,ROUND(Comp.AVG_CompPrice,2) AS Current_Week_Avg_Wiser_Comp_Price
    ,ROUND(Comp.Lowest_Comp_Price,2) AS Current_Week_Lowest_Comp_Price
    ,Comp.CompName AS Current_Week_CompName
    ,CONCAT('$',CAST(ROUND(L.Sales,2) AS STRING)) AS Last_Week_Sales
    ,L.Sales_Unit AS Last_Week_Sales_Unit
    ,L.EcomStoreSnWrUnits AS Last_Week_Ecom_Store_Sales_Units
	,L.EcomSnWrUnits AS Last_Week_Ecom_Sales_Unit
    ,ROUND(L.AUSP,2) AS Last_Week_AUSP
    ,CONCAT('$',CAST(ROUND(L.TC_D*100,2) AS STRING)) AS Last_Week_TC_D
    ,CONCAT(CAST(ROUND(L.TC_P*100,2) AS STRING),'%') AS Last_Week_TC_P
    ,CONCAT(CAST(ROUND(L.TC_P_BPS_Change_LY,2) AS STRING),'%') AS Last_Week_TC_P_BPS_Change_LY
    ,ROUND(L.CyPPSF,2) AS Last_Week_Ppsf
    ,(CASE WHEN COALESCE(LWPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(L.EcomStoreSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(LWPD.Traffic,0) END) AS Last_Week_Ecom_Store_Conversion
	,(CASE WHEN COALESCE(LWPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(L.EcomSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(LWPD.Traffic,0) END) AS Last_Week_Ecom_Conversion
    ,LWPD.Traffic AS Last_Week_Traffic
    ,ADL.Diff AS Last_Week_ATP
    ,CONCAT('$',CAST(ROUND(LWF.FRCST_Sales/1000000,2) AS STRING)) AS Last_Week_FRCST_Sales
    ,CONCAT('$',CAST(ROUND(LWF.FRCST_TC_D/1000000,2) AS STRING)) AS Last_Week_FRCST_TC_D
    ,CONCAT(CAST(ROUND(LWF.FRCST_TC_P/100,2) AS STRING),'%') AS Last_Week_FRCST_TC_P
    ,LRM.RevMix AS Last_Week_Rev_Mix
    ,CONCAT('$',CAST(ROUND(Y.Sales,2) AS STRING)) AS YTD_Sales
    ,CONCAT(CAST(ROUND(Y.Pct_Sales_CH_LY*100,2) AS STRING),'%') AS YTD_Pct_Sales_CH_LY
    ,Y.Sales_Unit AS YTD_Sales_Unit
    ,Y.EcomStoreSnWrUnits AS YTD_Ecom_Store_Sales_Units
	,Y.EcomSnWrUnits AS YTD_Ecom_Sales_Unit
    ,CONCAT(CAST(ROUND(Y.Pct_Sales_Unit_CH_LY*100,2) AS STRING),'%') AS YTD_Pct_Sales_Unit_CH_LY
    ,ROUND(Y.AUSP,2) AS YTD_AUSP
    ,CONCAT(CAST(ROUND(Y.Pct_AUSP_CH_LY*100,2) AS STRING),'%') AS YTD_Pct_AUSP_CH_LY
    ,CONCAT('$',CAST(ROUND(Y.TC_D*100,2) AS STRING)) YTD_TC_D
    ,CONCAT(CAST(ROUND(Y.TC_P*100,2) AS STRING),'%') AS YTD_TC_P
    ,CONCAT(CAST(ROUND(Y.TC_P_BPS_Change_LY,2) AS STRING),'%') AS YTD_TC_P_BPS_Change_LY
    --,ROUND(Y.CyPPSF,2) AS YTD_Ppsf
    ,(CASE WHEN COALESCE(YPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(Y.EcomStoreSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(YPD.Traffic,0) END) AS YTD_Ecom_Store_Conversion
	,(CASE WHEN COALESCE(YPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(Y.EcomSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(YPD.Traffic,0) END) AS YTD_Ecom_Conversion
    ,YPD.Traffic AS YTD_Traffic
    ,CONCAT('$',CAST(ROUND(YTDF.FRCST_Sales/1000000,2) AS STRING)) AS YTD_FRCST_Sales
    ,CONCAT('$',CAST(ROUND(YTDF.FRCST_TC_D/1000000,2) AS STRING)) AS YTD_FRCST_TC_D
    ,CONCAT(CAST(ROUND(YTDF.FRCST_TC_P/100,2) AS STRING),'%') AS YTD_FRCST_TC_P
    ,YRM.RevMix AS YTD_Rev_Mix
    ,CONCAT('$',CAST(ROUND(Q.Sales,2) AS STRING)) AS QTD_Sales
    ,CONCAT(CAST(ROUND(Q.Pct_Sales_CH_LY*100,2) AS STRING),'%') AS QTD_Pct_Sales_CH_LY
    ,Q.Sales_Unit AS QTD_Sales_Unit
    ,Q.EcomStoreSnWrUnits AS QTD_Ecom_Store_Sales_Units
	,Q.EcomSnWrUnits AS QTD_Ecom_Sales_Unit
    ,CONCAT(CAST(ROUND(Q.Pct_Sales_Unit_CH_LY*100,2) AS STRING),'%') AS QTD_Pct_Sales_Unit_CH_LY
    ,ROUND(Q.AUSP,2) AS QTD_AUSP
    ,CONCAT(CAST(ROUND(Q.Pct_AUSP_CH_LY*100,2) AS STRING),'%') AS QTD_Pct_AUSP_CH_LY
    ,CONCAT('$',CAST(ROUND(Q.TC_D,2) AS STRING)) AS QTD_TC_D
    ,CONCAT(CAST(ROUND(Q.TC_P*100,2) AS STRING),'%') AS QTD_TC_P
    ,CONCAT(CAST(ROUND(Q.TC_P_BPS_Change_LY,2) AS STRING),'%') AS QTD_TC_P_BPS_Change_LY
    --,ROUND(Q.CyPPSF,2) AS QTD_Ppsf
    ,(CASE WHEN COALESCE(QPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(Q.EcomStoreSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(QPD.Traffic,0) END) AS QTD_Ecom_Store_Conversion
	,(CASE WHEN COALESCE(QPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(Q.EcomSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(QPD.Traffic,0) END) AS QTD_Ecom_Conversion
    ,QPD.Traffic AS QTD_Traffic
    ,CONCAT('$',CAST(ROUND(QTDF.FRCST_Sales/1000000,2) AS STRING)) AS QTD_FRCST_Sales
    ,CONCAT('$',CAST(ROUND(QTDF.FRCST_TC_D/1000000,2) AS STRING)) AS QTD_FRCST_TC_D
    ,CONCAT(CAST(ROUND(QTDF.FRCST_TC_P/100,2) AS STRING),'%') AS QTD_FRCST_TC_P
    ,QRM.RevMix AS QTD_Rev_Mix
    ,CONCAT('$',CAST(ROUND(P.Sales,2) AS STRING)) AS PTD_Sales
    ,CONCAT(CAST(ROUND(P.Pct_Sales_CH_LY*100,2) AS STRING),'%') AS PTD_Pct_Sales_CH_LY
    ,P.Sales_Unit AS PTD_Sales_Unit
    ,P.EcomStoreSnWrUnits AS PTD_Ecom_Store_Sales_Units
	,P.EcomSnWrUnits AS PTD_Ecom_Sales_Unit
    ,CONCAT(CAST(ROUND(P.Pct_Sales_Unit_CH_LY*100,2) AS STRING),'%') AS PTD_Pct_Sales_Unit_CH_LY
    ,ROUND(P.AUSP,2) AS PTD_AUSP
    ,CONCAT(CAST(ROUND(P.Pct_AUSP_CH_LY*100,2) AS STRING),'%') AS PTD_Pct_AUSP_CH_LY
    ,CONCAT('$',CAST(ROUND(P.TC_D,2) AS STRING)) AS PTD_TC_D
    ,CONCAT(CAST(ROUND(P.TC_P*100,2) AS STRING),'%') AS PTD_TC_P
    ,CONCAT(CAST(ROUND(P.TC_P_BPS_Change_LY,2) AS STRING),'%') AS PTD_TC_P_BPS_Change_LY
    --,ROUND(P.CyPPSF,2) AS PTD_Ppsf
    ,(CASE WHEN COALESCE(PPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(Q.EcomStoreSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(PPD.Traffic,0) END) AS PTD_Ecom_Store_Conversion
	,(CASE WHEN COALESCE(PPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(Q.EcomSnWrUnits,0)) OVER(PARTITION BY A.ITEM_NUMBER))/COALESCE(PPD.Traffic,0) END) AS PTD_Ecom_Conversion
    ,PPD.Traffic AS PTD_Traffic
    ,CONCAT('$',CAST(ROUND(PTDF.FRCST_Sales/1000000,2) AS STRING)) AS PTD_FRCST_Sales
    ,CONCAT('$',CAST(ROUND(PTDF.FRCST_TC_D/1000000,2) AS STRING)) AS PTD_FRCST_TC_D
    ,CONCAT(CAST(ROUND(PTDF.FRCST_TC_P/100,2) AS STRING),'%') AS PTD_FRCST_TC_P
    ,PRM.RevMix AS PTD_Rev_Mix
    ,'dbt_pricing_grid_product_level_sales_kpis' AS CreatedBy
    ,'dbt_pricing_grid_product_level_sales_kpis' AS ModifiedBy
    ,CURRENT_DATETIME("America/Chicago") AS CreatedDateTime
    ,CURRENT_DATETIME("America/Chicago") AS ModifiedDateTime
FROM 
	{{ref('pricing_grid_product_level_sales_kpis_tbl1_stg')}} A
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_lweek_stg')}} L
ON 
	A.Variant_Id = L.ProductId
LEFT JOIN 
(
    SELECT
        CAST(AVG(Diff) AS INT64) AS Diff
        ,Id
        ,MAX(FirstAvailableDeliveryDateAsOf) AS FirstAvailableDeliveryDateAsOf
    FROM
	(
        SELECT
            CAST(FLOOR(AVG(-(DATE_DIFF(ATPEarliestSlotDate, ATPEarliestQtyDate, DAY)))) AS INT64) AS Diff
            ,VariantID AS Id
            ,ATPEarliestSlotDate AS FirstAvailableDeliveryDateAsOf
        FROM 
			{{source('mfrm_sales_dynamic', 'atp_first_available_dates')}}
        WHERE 
			EXTRACT(WEEK FROM ATPEarliestSlotDate) = (SELECT FWeekNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
			AND EXTRACT(YEAR FROM ATPEarliestSlotDate) = (SELECT FYearNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
        GROUP BY 2,3
    )
	GROUP BY 2
) AD
ON 
	A.Variant_Id = AD.Id
LEFT JOIN 
(
    SELECT
        CAST(AVG(Diff) AS INT64) AS Diff
        ,Id
        ,MAX(FirstAvailableDeliveryDateAsOf) AS FirstAvailableDeliveryDateAsOf
    FROM
	(
        SELECT
            CAST(FLOOR(AVG(-(DATE_DIFF(ATPEarliestSlotDate, ATPEarliestQtyDate, DAY)))) AS INT64) AS Diff
            ,VariantID AS Id
            ,ATPEarliestSlotDate AS FirstAvailableDeliveryDateAsOf
        FROM 
			{{source('mfrm_sales_dynamic', 'atp_first_available_dates')}}
        WHERE 
			EXTRACT(WEEK FROM ATPEarliestSlotDate) = (SELECT FWeekNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())-1
			AND EXTRACT(YEAR FROM ATPEarliestSlotDate) = (SELECT FYearNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
        GROUP BY 2,3
    )
	GROUP BY 2
) ADL
ON 
	A.Variant_Id = ADL.Id
LEFT JOIN
(
    SELECT *
    FROM(
        SELECT
            VariantID
            ,CompName
            ,AVG(CompPrice) OVER(PARTITION BY VariantID) AS AVG_CompPrice
            ,MIN(CompPrice)  OVER(PARTITION BY VariantID) AS Lowest_Comp_Price
            ,ROW_NUMBER() OVER (PARTITION BY VariantID ORDER BY ModifiedDateTime DESC) AS R
        FROM 
			{{source('mfrm_sales_dynamic', 'vw_competitive_insights')}}
        )
    WHERE R=1
) Comp
    ON Comp.VariantID = A.Variant_ID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_ytd_stg')}} Y
ON 
	Y.ProductId = A.Variant_ID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_qtd_stg')}} Q
ON 
	Q.ProductId = A.Variant_ID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_ptd_stg')}} P
ON 
	P.ProductId = A.Variant_ID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_ytd_product_details_stg')}} YPD
ON 
	A.Item_Number = YPD.VariantID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_qtd_product_details_stg')}} QPD
ON 
	A.Item_Number = QPD.VariantID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_ptd_product_details_stg')}} PPD
ON 
	A.Item_Number = PPD.VariantID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_lweek_product_details_stg')}} LWPD
ON 
	LWPD.VariantID = A.Item_Number
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_cweek_frcst_stg')}} CWF
ON 
	CWF.ProductId = A.Variant_Id
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_lweek_frcst_stg')}} LWF
ON 
	LWF.ProductId = A.Variant_Id
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_ytd_frcst_stg')}} YTDF
ON 
	YTDF.ProductId = A.Variant_Id
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_cweek_frcst_stg')}} QTDF
ON 
	QTDF.ProductId = A.Variant_Id
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_cweek_frcst_stg')}} PTDF
ON 
	PTDF.ProductId = A.Variant_Id
LEFT JOIN 
    {{ref('pricing_grid_product_level_sales_kpis_lweek_rev_mix')}} LRM
ON 
    A.Variant_ID = LRM.ProductId
LEFT JOIN 
    {{ref('pricing_grid_product_level_sales_kpis_ytd_rev_mix')}} YRM
ON 
    A.Variant_ID = YRM.ProductId
LEFT JOIN 
    {{ref('pricing_grid_product_level_sales_kpis_qtd_rev_mix')}} QRM
ON 
    A.Variant_ID = QRM.ProductId
LEFT JOIN 
    {{ref('pricing_grid_product_level_sales_kpis_ptd_rev_mix')}} PRM
ON 
    A.Variant_ID = PRM.ProductId