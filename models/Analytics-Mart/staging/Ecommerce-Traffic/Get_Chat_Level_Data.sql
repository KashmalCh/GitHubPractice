{{config(
    materialized = 'table',
    schema = 'stg'
)}}


WITH Get_Chat_Level_Data AS
(
SELECT
d.CalendarDate,
d.fyearweek SnapshotWrittenWeek,
d.FYearNo AS FYear,
d.FWeekNo AS FWeek,
d.FPeriodNo AS FPeriod,
d.FQtrNo AS FQtr,
SUM(ws.sales) AS  Chat_Rev,
(SUM(sales)-COALESCE(SUM(TtlCoreCost),0)) AS Chat_Margin_half,
COUNT(distinct ws.Salesid) AS Chat_Trans,

FROM `mfrm_sales.dim_calendar` d
    LEFT JOIN `mfrm_sales.fact_sales_written_snapshot` ws
    ON ws.WrittenDate = d.CalendarDate
    INNER JOIN `mfrm_sales.store_master` ds 
      ON ws.storeid = ds.storeid
	INNER JOIN `mfrm_sales.product_master` dp 
      ON ws.productid = dp.VariantId
	AND ds.PurchaseChannel = 'Chat'
	AND   d.CalendarDate < CURRENT_DATE()
    AND   (lower(ds.division) != 'corporate' OR lower(ds.Region) != 'corporate' OR lower(ds.Market) != 'corporate')
    AND   ifnull(dp.IsExclusionItem ,FALSE) = FALSE
    AND ( Case when LOWER(ltrim(rtrim(dp.Disposition))) = '' then null else LOWER(ltrim(rtrim(dp.Disposition))) END IN (LOWER('PRIME'),LOWER('OUTLET'),LOWER('DISPLAY'),LOWER('FLOOR'))
    OR Case when ltrim(rtrim(dp.Disposition)) = '' then null else ltrim(rtrim(dp.Disposition)) END IS NULL)
    AND ws.ProductId IS NOT NULL
    GROUP BY 1,2,3,4,5,6--,7,8
)
Select a.CalendarDate,	
SnapshotWrittenWeek,	
FYear,	
FWeek,	
FPeriod,	
FQtr,	
Chat_Rev AS  Chat_Rev,	
(COALESCE(Chat_Margin_half,0)+COALESCE(SnWrVI,0)) Chat_Margin,	
Chat_Trans AS Chat_Trans	
from Get_Chat_Level_Data	a
join
(
SELECT
d.CalendarDate,

CAST(SUM(ws.ttlcorecost*dp.vipctrate) AS NUMERIC) SnWrVi,

FROM `mfrm_sales.dim_calendar` d
    LEFT JOIN `mfrm_sales.fact_sales_written_snapshot` ws
    ON ws.WrittenDate = d.CalendarDate
    INNER JOIN `mfrm_sales.store_master` ds 
      ON ws.storeid = ds.storeid
	INNER JOIN `mfrm_sales.product_master` dp 
      ON ws.productid = dp.VariantId
	AND ds.PurchaseChannel = 'Chat'
	AND   d.CalendarDate < CURRENT_DATE()
    AND   (lower(ds.division) != 'corporate' OR lower(ds.Region) != 'corporate' OR lower(ds.Market) != 'corporate')
    AND   ifnull(dp.IsExclusionItem ,FALSE) = FALSE
    AND ( Case when LOWER(ltrim(rtrim(dp.Disposition))) = '' then null else LOWER(ltrim(rtrim(dp.Disposition))) END IN (LOWER('PRIME'),LOWER('OUTLET'),LOWER('DISPLAY'),LOWER('FLOOR'))
    OR Case when ltrim(rtrim(dp.Disposition)) = '' then null else ltrim(rtrim(dp.Disposition)) END IS NULL)
    AND ws.ProductId IS NOT NULL
    GROUP BY 1
)b
on
a.CalendarDate=b.CalendarDate
--where FYear = 2021 and FWeek = 40
order by a.CalendarDate