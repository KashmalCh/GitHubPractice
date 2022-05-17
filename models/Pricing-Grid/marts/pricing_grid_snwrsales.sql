{{config(
    materialized = 'incremental'
    ,unique_key = 'UniqueKey'
)}}
SELECT 
      CONCAT(CAST(d.fyearweek AS STRING),ws.ProductId) AS UniqueKey,
      d.fyearweek SnapshotWrittenWeek,
      ws.ProductId,
      d.FYearNo AS FYear,
      d.FWeekNo AS FWeek,
      d.FPeriodNo AS FPeriod,
      d.FQtrNo AS FQtr,
      SUM(CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN ws.sales
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN ws.sales
        ELSE NULL END) SnWrSales,
      SUM(CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN ws.qty
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN ws.qty
        ELSE NULL END) SnWrUnits,
      SUM(CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay AND ws.StoreId = '290007' THEN ws.qty
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 AND ws.StoreId = '290007' THEN ws.qty
        ELSE NULL END) EcomStoreSnWrUnits,
      SUM(CASE WHEN LOWER(ds.Division) = 'ecommerce' THEN ws.qty
        ELSE NULL END) EcomSnWrUnits,
      SUM(CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN ws.unitprice
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN ws.unitprice
        ELSE NULL END) SnWrUnitPrice,
      SUM(CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN ws.cost
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN ws.cost
        ELSE NULL END) SnWrPosCost,
      SUM(CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN ws.TtlCoreCost
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN ws.TtlCoreCost
        ELSE NULL END) SnWrCoreCost,
      CAST(SUM(CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN dp.OTP*ws.Qty
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN dp.OTP*ws.Qty
        ELSE NULL END) AS NUMERIC) SnWrOlp,
      SUM(CASE WHEN ws.IsReturn = TRUE AND d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN ws.sales
        WHEN ws.IsReturn = TRUE AND d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN ws.sales
        ELSE NULL END) SnWrReturnSales,
      CAST(SUM(ws.ttlcorecost*dp.vipctrate) AS NUMERIC) SnWrVi,
      COUNT(DISTINCT CASE WHEN d.fyearweek = (dr.MaxFyearWeek-100) AND dr.DayRank < dr.MaxDay THEN ws.Salesid
        WHEN d.fyearweek <> (dr.MaxFyearWeek-100) AND dr.DayRank <= 7 THEN ws.Salesid
        ELSE NULL END) SnWrSalesTicketCount
      ,'dbt_pricing_grid_snwrsales' AS CreatedBy
      ,'dbt_pricing_grid_snwrsales' AS ModifiedBy
      ,CURRENT_DATETIME("America/Chicago") AS CreatedDateTime
      ,CURRENT_DATETIME("America/Chicago") AS ModifiedDateTime
    FROM {{source('mfrm_sales_dynamic', 'fact_sales_written_snapshot')}} ws
    LEFT JOIN {{source('mfrm_sales_dynamic', 'dim_calendar')}} d
    ON ws.WrittenDate = d.CalendarDate
    LEFT JOIN {{source('mfrm_sales_dynamic', 'store_master')}} ds 
      ON ws.storeid = ds.storeid
    LEFT JOIN {{source('mfrm_sales_dynamic', 'product_master')}} dp 
      ON ws.productid = dp.VariantId
    LEFT JOIN {{source('mfrm_sales_dynamic', 'vw_pricing_grid_day_rank')}} dr 
      ON dr.CalendarDate = d.CalendarDate
    LEFT JOIN {{source('mfrm_sales_dynamic', 'vw_pricing_grid_category')}} pb 
      ON ws.unitprice > pb.PriceMin and ws.unitprice <= pb.PriceMax
    WHERE d.fYearPeriod IN (SELECT YearPeriod FROM {{source('mfrm_sales_dynamic', 'vw_pricing_grid_two_fyear_period')}})
    AND   d.CalendarDate < CURRENT_DATE()
    AND   (lower(ds.division) != 'corporate' OR lower(ds.Region) != 'corporate' OR lower(ds.Market) != 'corporate')
    AND   ifnull(dp.IsExclusionItem ,FALSE) = FALSE
    AND ( Case when LOWER(ltrim(rtrim(dp.Disposition))) = '' then null else LOWER(ltrim(rtrim(dp.Disposition))) END IN (LOWER('PRIME'),LOWER('OUTLET'),LOWER('DISPLAY'),LOWER('FLOOR'))
    OR Case when ltrim(rtrim(dp.Disposition)) = '' then null else ltrim(rtrim(dp.Disposition)) END IS NULL)
    AND ws.ProductId IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7
