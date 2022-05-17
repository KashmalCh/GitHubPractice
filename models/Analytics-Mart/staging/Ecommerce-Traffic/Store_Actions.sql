{{config(
    materialized = 'table',
    schema = 'stg'
)}}

WITH Store_Actions AS
(
SELECT 
    dc.CalendarDate,
    dc.FYearNo AS FYear,
    dc.FWeekNo AS FWeek,
    dc.FPeriodNo AS FPeriod,
    dc.FQtrNo AS FQtr, 
    count(distinct CONCAT(fullVisitorId, CAST(visitStartTime AS STRING) )) Store_Sessions
FROM 
    `mattress-firm-inc.8460582.ga_sessions_*`  ga
    LEFT JOIN UNNEST(hits) h
    join `mattress-firm-inc.mfrm_sales.dim_calendar` dc on PARSE_DATE("%Y%m%d", ga.date) = dc.CalendarDate
WHERE 
    _TABLE_SUFFIX >= '20180101'
    and REGEXP_CONTAINS(h.eventInfo.eventCategory,"^Find A Store \\(Header\\)$|^Store Locator Card$|^Store Locator$|^Zip Update$")
    and REGEXP_CONTAINS(h.eventInfo.eventAction,"^Directions|^Page View$|^click$")
group by 
    dc.CalendarDate,
    dc.FYearNo ,
    dc.FWeekNo ,
    dc.FPeriodNo ,
    dc.FQtrNo
)
Select * FROM Store_Actions