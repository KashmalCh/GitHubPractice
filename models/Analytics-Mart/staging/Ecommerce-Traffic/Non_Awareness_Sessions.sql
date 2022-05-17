{{config(
    materialized = 'table',
    schema = 'stg'
)}}

WITH Non_Awareness_Sessions AS
(
select
    fyearperiod,
    fyearweek,
    CalendarDate,
    FYearNo AS FYear,
    FWeekNo AS FWeek,
    FPeriodNo AS FPeriod,
    FQtrNo AS FQtr, 
    'Ecommerce' division_group,
    count(distinct concat(clientId,visitId)) Non_Awareness_Sessions
from 
    `mattress-firm-inc.8460582.ga_sessions_*` ga ,
    unnest(hits) as hits
    join `mattress-firm-inc.mfrm_sales.dim_calendar` dc on PARSE_DATE("%Y%m%d", date) = dc.CalendarDate
where
    _TABLE_SUFFIX >= '20180101'                                   
    and totals.visits = 1
    and not regexp_contains(trafficSource.campaign,'awareness')
    and hits.page.hostname not like '%dev%'
    and hits.page.hostname not like '%test%'
    and hits.page.pagePath not like '%whoops%'
    and hits.page.hostname is not null
    and not regexp_contains(hits.page.pagePath,'synthetictest|001038')
    and hits.page.hostname not like '%dynamics.com%'
    and hits.page.hostname not like '%staging%'
    and hits.page.pagePath not like '%/es/%'
    and lower(trafficSource.campaign) not like '%span_%' 
	
	and hits.page.hostname not like '%source=qr%'
	and hits.page.hostname not like '%ab=true%'
	and hits.page.hostname not like '%yotta%'
	and not regexp_contains(hits.page.pagePath,'source=qr|ab=true|yotta')
group by
    fyearperiod,
    fyearweek,
    CalendarDate,
    FYearNo,
    FWeekNo,
    FPeriodNo,
    FQtrNo
)
SELECT * FROM Non_Awareness_Sessions