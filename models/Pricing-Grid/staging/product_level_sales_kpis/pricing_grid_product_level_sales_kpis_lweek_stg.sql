{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT DISTINCT
    A.ProductId
    ,SUM(SnWrSales) AS Sales
    ,(SUM(SnWrSales)-SUM(LySnWrSales))/(CASE WHEN (SUM(SnWrSales)+SUM(LySnWrSales)) = 0 THEN 1 ELSE (SUM(SnWrSales)+SUM(LySnWrSales)) END) AS Pct_Sales_CH_LY
    ,SUM(SnWRUnits) AS Sales_Unit
    ,(SUM(SnWRUnits)-SUM(LySnWRUnits))/(CASE WHEN (SUM(SnWRUnits)+SUM(LySnWRUnits)) = 0 THEN 1 ELSE (SUM(SnWRUnits)+SUM(LySnWRUnits)) END) AS Pct_Sales_Unit_CH_LY
    ,(CASE WHEN SUM(SnWRUnits) = 0 THEN 0 ELSE SUM(SnWrSales)/SUM(SnWRUnits) END) AS AUSP
    ,CASE WHEN ((CASE WHEN SUM(SnWRUnits) = 0 THEN 0 ELSE SUM(SnWrSales)/SUM(SnWRUnits) END)+(CASE WHEN SUM(LySnWRUnits) = 0 THEN 0 ELSE SUM(LySnWrSales)/SUM(LySnWRUnits) END)) = 0 THEN 0 ELSE ((CASE WHEN SUM(SnWRUnits) = 0 THEN 0 ELSE SUM(SnWrSales)/SUM(SnWRUnits) END)-(CASE WHEN SUM(LySnWRUnits) = 0 THEN 0 ELSE SUM(LySnWrSales)/SUM(LySnWRUnits) END))/
    ((CASE WHEN SUM(SnWRUnits) = 0 THEN 0 ELSE SUM(SnWrSales)/SUM(SnWRUnits) END)+(CASE WHEN SUM(LySnWRUnits) = 0 THEN 0 ELSE SUM(LySnWrSales)/SUM(LySnWRUnits) END)) END AS Pct_AUSP_CH_LY
    ,((SUM(SnWrSales)-COALESCE(SUM(SnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0))/100 AS TC_D
    ,((((SUM(SnWrSales)-COALESCE(SUM(SnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)))/(CASE WHEN SUM(SnWrSales) = 0 THEN 1 ELSE SUM(SnWrSales) END))/100 AS TC_P
    ,(
    (((SUM(SnWrSales)-COALESCE(SUM(SnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0))/(CASE WHEN SUM(SnWrSales) = 0 THEN 1 ELSE SUM(SnWrSales) END))
    -(((SUM(LySnWrSales)-COALESCE(SUM(LySnWrCoreCost),0))+COALESCE(SUM(LySnWrVI),0))/(CASE WHEN SUM(LySnWrSales) = 0 THEN 1 ELSE SUM(LySnWrSales) END))
    )*10000 TC_P_BPS_Change_LY
    ,((SUM(SnWrSales)-COALESCE(SUM(SnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END)/100 AS CyPPSF
    ,((SUM(LySnWrSales)-COALESCE(SUM(LySnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END)/100 AS LyPPSF
    ,(((SUM(SnWrSales)-COALESCE(SUM(SnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END)-
    ((SUM(LySnWrSales)-COALESCE(SUM(LySnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END))/(
        CASE WHEN 
    (((SUM(SnWrSales)-COALESCE(SUM(SnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END)+
    ((SUM(LySnWrSales)-COALESCE(SUM(LySnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END))=0 THEN 1 ELSE 
    (((SUM(SnWrSales)-COALESCE(SUM(SnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END)+
    ((SUM(LySnWrSales)-COALESCE(SUM(LySnWrCoreCost),0))+COALESCE(SUM(SnWrVI),0)/CASE WHEN SUM(B.Sqft)=0 THEN 1 ELSE SUM(B.Sqft) END)) END) AS PPSF_CH_LY
    ,SUM(SnWrUnits) AS SnWrUnits
	,SUM(EcomStoreSnWrUnits) AS EcomStoreSnWrUnits
    ,SUM(EcomSnWrUnits) AS EcomSnWrUnits
FROM 
	{{ref('pricing_grid_sales_update')}} A
LEFT JOIN 
	(
        SELECT 
			ProductID
			,(COALESCE(A.QUEEN,0)*33.33)+(COALESCE(A.TWIN,0)*20.31)+(COALESCE(A.KING,0)*43.33)+(COALESCE(A.TWINXL,0)*21.66)+(COALESCE(A.FULL,0)*28.13)+(COALESCE(A.ValueZone,0)*3.5) AS SQFT
		FROM(
			SELECT
				A.ProductID
				,SUM(COALESCE(B.QUEEN,0)) AS QUEEN
				,SUM(COALESCE(B.TWIN,0)) AS TWIN
				,SUM(COALESCE(B.KING,0)) AS KING
				,SUM(COALESCE(B.TWINXL,0)) AS TWINXL
				,SUM(COALESCE(B.FULL,0)) AS `FULL`
				,CASE WHEN (SUM(COALESCE(A.OnHandQty,0))
				-(SUM(COALESCE(B.QUEEN,0))+SUM(COALESCE(B.TWIN,0))+SUM(COALESCE(B.KING,0))+SUM(COALESCE(B.TWINXL,0))+SUM(COALESCE(B.FULL,0))))<0
				THEN 0 ELSE (SUM(COALESCE(A.OnHandQty,0))
				-(SUM(COALESCE(B.QUEEN,0))+SUM(COALESCE(B.TWIN,0))+SUM(COALESCE(B.KING,0))+SUM(COALESCE(B.TWINXL,0))+SUM(COALESCE(B.FULL,0))))
				END AS ValueZone
			FROM {{source('mfrm_sales_dynamic', 'fact_day_iat_inventory')}} A
			LEFT JOIN (
				SELECT
					A.OrderBrand
					,A.ModelName
					,A.QUEEN
					,A.TWIN
					,A.KING
					,A.TWINXL
					,A.FULL
				FROM(
					SELECT *
					FROM (
						SELECT
							PrimaryVendorID AS OrderBrand
							,NameAlias AS ModelName
							,InventSizeID AS Size
							,OnHandQty AS Qty
						FROM `mfrm_sales.fact_day_iat_inventory`
					)
					PIVOT(
						SUM(Qty) FOR Size IN ('QUEEN','KING','TWIN','TWINXL','FULL')
					) AS A)A
				)B
			ON A.PrimaryVendorID = B.OrderBrand
			AND A.NameAlias = B.ModelName
			GROUP BY 1
			) A
    ) B
ON 
	A.ProductID = B.ProductID
WHERE 
	FYear = (SELECT FYearNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())
	AND FWeek = (SELECT FWeekNo FROM `mfrm_sales.dim_calendar` WHERE CalendarDate = CURRENT_DATE())-1
	AND A.ProductId IS NOT NULL
GROUP BY 
	A.ProductId