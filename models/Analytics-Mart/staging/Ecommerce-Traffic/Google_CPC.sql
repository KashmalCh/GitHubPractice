{{config(
    materialized = 'table',
    schema = 'stg'
)}}

WITH Google_CPC AS
(
Select 
a.Date,
sum(cost/1000000)/sum(clicks) AS Google_CPC,
b.CalendarDate,
b.fyearperiod,
b.fyearweek,
b.FYearNo,
b.FWeekNo,
b.FPeriodNo,
b.FQtrNo, 
FROM 
mattress-firm-inc.mfrm_google_ads_dataset_2139348461.p_CampaignBasicStats_2139348461 a
LEFT join `mattress-firm-inc.mfrm_sales.dim_calendar` b
on a.Date = b.CalendarDate
GROUP BY 
a.Date,
b.CalendarDate,
b.CalendarDate,
b.fyearperiod,
b.fyearweek,
b.FYearNo,
b.FWeekNo,
b.FPeriodNo,
b.FQtrNo
)
Select * from Google_CPC
