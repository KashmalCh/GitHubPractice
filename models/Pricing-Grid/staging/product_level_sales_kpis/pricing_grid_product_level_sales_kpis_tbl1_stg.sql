{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT DISTINCT
    A.Vendor
    ,A.VendorBrand AS Vendor_Brand
    ,A.ModelName AS Model_Name
    ,CASE WHEN CAST(TRIM(MerchandisingSales) AS NUMERIC)>=2000 AND A.Type = 'MATTRESS' THEN 'Luxury - 2000+'
    WHEN CAST(TRIM(MerchandisingSales) AS NUMERIC)<2000 AND CAST(TRIM(MerchandisingSales) AS NUMERIC)>=1000 AND A.Type = 'MATTRESS' THEN 'Premium - 1000 - 1999'
    WHEN CAST(TRIM(MerchandisingSales) AS NUMERIC)<1000 AND CAST(TRIM(MerchandisingSales) AS NUMERIC)>=500 AND A.Type = 'MATTRESS' THEN 'Promo - 500 - 999'
    WHEN CAST(TRIM(MerchandisingSales) AS NUMERIC)<500 AND CAST(TRIM(MerchandisingSales) AS NUMERIC)>0 AND A.Type = 'MATTRESS' THEN 'Value - 0-499'
    ELSE NULL END AS Price_Brand
    ,A.Channel
    ,A.Mapp
    ,A.ProductType1 AS Type
    ,A.ProductType2 AS Product_Type
    ,A.SEGMENTAX AS Segment
    ,A.ProductName AS Product_Name
    ,A.Size
    ,A.Box
    ,A.CoreCost AS Core_Cost
    ,A.COREFROMDATE AS Core_From_Date
    ,A.CORETODATE AS Core_To_Date
    ,A.POSCost AS POS_Cost
    ,A.OLP
    ,A.MerchandisingSales AS Merchandising_Sales
    ,A.FromDate AS Merchandising_Sales_From_Date
    ,A.ToDate AS Merchandising_Sales_To_Date
    ,A.VendorSKU AS Vendor_Sku
    ,A.GTINCode AS GTIN_Code
    ,A.Site
    ,A.AccountSelection AS Account_Selection
    ,A.InventStopped AS Invent_Stopped
    ,A.PurchaseStopped AS Purchase_Stopped
    ,A.DirectDelivery AS Direct_Delivery
    ,A.ZeroPriceValid AS Zero_Price_Valid
    ,A.NoDiscountAllowed AS No_Discount_Allowed
    ,A.DATEBLOCKED AS Blocked_at_Registered
    ,A.DATEBLOCKED AS Date_Blocked
    ,A.ItemSalesStopped AS Item_Status_Stopped
    ,A.MODEOFDELIVERY AS Mode_of_Delivery
    ,A.Category
    ,A.Comfort
    ,A.Construction
    ,A.Active
    ,A.QueenPricePoint AS Queen_Price_Point
    ,A.Class
    ,A.Domain
    ,A.MatchingBox AS Matching_Box
    ,A.ProductStyle AS Product_Style
    ,A.Replaces
    ,A.WarrantyPeriod AS Warranty_Period
    ,A.CBCClassification AS CBC_Classification
    ,A.CompensationReporting AS Compensation_Reporting
    ,A.DiscontinuedDate AS Discontinued_Date
    ,A.PrivateLabel AS Private_Label
    ,A.RolloutDate AS Rollout_Date
    ,A.SalesReporting AS Sales_Reporting
    ,A.SubsidyEffectiveRate AS Subsidy_Effective_Rate
    ,A.DISPATCHTRACK AS Dispatch_Track
    ,A.ADJUSTABLEFRIENDLY AS Adjustable_Friendly
    ,A.MODEOFDELIVERY AS Mode_of_Delivery1
    ,A.PurchaseStoppedDate AS Purchase_Stopped_Date
    ,A.Placement
    ,A.FloorSampleDiscount AS Floor_Sample_Discount
    ,A.OutletDepreciation AS Outlet_Depreciation
    ,A.BarcodeDescription AS Barcode_Description
    ,A.ItemPurchasedStopped AS Item_Purchased_Stopped
    ,A.ItemInventoryStopped AS Item_Inventory_Stopped
    ,A.ItemSalesStopped AS Item_Sales_Stopped
    ,A.LinkedItemNumber AS Linked_Item_Number
    ,A.LinkedProductName AS Linked_Product_Name
    ,A.LinkQty AS Link_Qty
    ,A.LinkVariant AS Link_Variant
    ,A.Freight
    ,A.Exclusive
    ,A.VariantId AS Variant_Id
    ,CAST(A.ITEMNUMBER AS STRING) AS Item_Number
    ,CONCAT('$',CAST(ROUND(C.Sales,2) AS STRING)) AS Current_Week_Sales
    ,CONCAT(CAST(ROUND(C.Pct_Sales_CH_LY*100,2) AS STRING),'%') AS Current_Week_Pct_Sales_CH_LY
    ,CONCAT(CAST(ROUND(D.Pct_Sales_WOW*100,2) AS STRING),'%') AS Current_Week_Pct_Sales_WOW
    ,C.Sales_Unit AS Current_Week_Sales_Unit
	,C.EcomStoreSnWrUnits AS Current_Week_Ecom_Store_Sales_Units
	,C.EcomSnWrUnits AS Current_Week_Ecom_Sales_Unit
    ,CONCAT(CAST(ROUND(C.Pct_Sales_Unit_CH_LY*100,2) AS STRING),'%') AS Current_Week_Pct_Sales_Unit_CH_LY
    ,CONCAT(CAST(ROUND(D.Pct_Sales_Unit_WOW*100,2) AS STRING),'%') AS Current_Week_Pct_Sales_Unit_WOW
    ,C.AUSP AS Current_Week_AUSP
    ,CONCAT(CAST(ROUND(C.Pct_AUSP_CH_LY*100,2) AS STRING),'%') AS Current_Week_Pct_AUSP_CH_LY
    ,CONCAT(CAST(ROUND(D.Pct_AUSP_WOW*100,2) AS STRING),'%') AS Current_Week_Pct_AUSP_WOW
    ,CONCAT('$',CAST(ROUND(C.TC_D,2) AS STRING)) AS Current_Week_TC_D
    ,CONCAT(CAST(ROUND(C.TC_P*100,2) AS STRING),'%') AS Current_Week_TC_P
    ,CONCAT(CAST(ROUND(C.TC_P_BPS_Change_LY*100,2) AS STRING),'%') AS Current_Week_TC_P_BPS_Change_LY
    ,CONCAT(CAST(ROUND(D.TC_P_BPS_Change_LY_WOW*100,2) AS STRING),'%') AS Current_Week_TC_P_BPS_Change_LY_WOW
	,(CASE WHEN COALESCE(CWPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(C.EcomStoreSnWrUnits,0)) OVER(PARTITION BY A.ITEMNUMBER))/COALESCE(CWPD.Traffic,0) END) AS Current_Week_Ecom_Store_Conversion
	,(CASE WHEN COALESCE(CWPD.Traffic,0) = 0 THEN 0 ELSE (SUM(COALESCE(C.EcomSnWrUnits,0)) OVER(PARTITION BY A.ITEMNUMBER))/COALESCE(CWPD.Traffic,0) END) AS Current_Week_Ecom_Conversion
    ,CWPD.Traffic AS Current_Week_Traffic
    ,CONCAT(CAST(ROUND(PDC.CH_LY*100,2) AS STRING),'%') AS Current_Week_Traffic_Pct_CH_LY
    ,PDW.WOW AS Current_Week_Traffic_WOW
    ,C.CyPPSF AS Current_Week_Ppsf
    ,CONCAT(CAST(ROUND(C.PPSF_CH_LY*100,2) AS STRING),'%') AS Current_Week_Ppsf_CH_LY
    ,CONCAT(CAST(ROUND(D.PPSF_CH_WOW*100,2) AS STRING),'%') AS Current_Week_Ppsf_CH_WOW
    ,CRM.RevMix AS Current_Week_Rev_Mix
FROM 
	(
		SELECT 
			*
		FROM(
				SELECT 
					A.*
					,ROW_NUMBER() OVER(PARTITION BY VARIANTID ORDER BY COREFROMDATE DESC) AS R
				FROM 
					{{source('mfrm_sales_dynamic', 'vw_ssrs_rpt_axproduct_sku')}} A
				WHERE 
					LOWER(Disposition) = LOWER('PRIME')
					AND LOWER(AccountSelection) = LOWER('ALL') AND A.Active = TRUE
					AND EXTRACT(YEAR FROM CORETODATE) = 2154
					AND EXTRACT(YEAR FROM TODATE) = 2154
			)
		WHERE 
			R=1
	) A
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_cweek_stg')}} C
ON 
	A.VARIANTID = C.ProductId
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_wow_stg')}} D
ON 
	A.VARIANTID = D.ProductId
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_cweek_product_details_stg')}} CWPD
ON 
	CWPD.VariantId = CAST(A.ITEMNUMBER AS STRING)
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_product_details_wow_stg')}} PDW
ON 
	PDW.VariantId = A.VARIANTID
LEFT JOIN 
	{{ref('pricing_grid_product_level_sales_kpis_product_details_ch_stg')}} PDC
ON 
	PDC.VariantId = A.VARIANTID
LEFT JOIN 
    {{ref('pricing_grid_product_level_sales_kpis_cweek_rev_mix')}} CRM
ON 
    CRM.ProductId = A.VARIANTID