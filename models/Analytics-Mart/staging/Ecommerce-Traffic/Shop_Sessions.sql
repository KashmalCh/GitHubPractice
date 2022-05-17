{{config(
    materialized = 'table',
    schema = 'stg'
)}}

WITH Shop_Sessions AS
(
SELECT
a.Segment,
a.Date,
a.Sessions AS Shop_Sessions,
b.fyearperiod,
b.fyearweek,
b.CalendarDate,
b.FYearNo,
b.FWeekNo,
b.FPeriodNo,
b.FQtrNo,
FROM 
mattress-firm-inc.mfrm_reporting_dataset.rpt_ga_purchase_propensity a
LEFT join `mattress-firm-inc.mfrm_sales.dim_calendar` b
on a.Date = b.CalendarDate
WHERE a.segment='Mattress Shopper'
GROUP BY 
a.Segment,
a.Date,
a.Sessions,
b.fyearperiod,
b.fyearweek,
b.CalendarDate,
b.FYearNo,
b.FWeekNo,
b.FPeriodNo,
b.FQtrNo
) Select * FROM Shop_Sessions