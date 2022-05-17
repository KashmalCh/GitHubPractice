{{config(
    materialized = 'table',
    schema = 'stg'
)}}

WITH  Get_PDP_Visits AS
(									
select									
Date,									
PDPVisits as PDP_Views									
From dev-mfrm-data.mfrm_reporting_dataset.rpt_ga_sessions_daily_aggr
)							
 								
                
, Get_PDP_Visits_Agg AS		
(								
select									
fyearperiod,
fyearweek,
CalendarDate,
FYearNo AS FYear,
FWeekNo AS FWeek,
FPeriodNo AS FPeriod,
FQtrNo AS FQtr,
sum(a.PDP_Views) as PDP_Views								
from									
`mattress-firm-inc.mfrm_sales.dim_calendar` dc								
left join Get_PDP_Visits as a on dc.CalendarDate = a.Date									
group by									
CalendarDate,
fyearperiod,
fyearweek,
CalendarDate,
FYearNo,
FWeekNo,
FPeriodNo,
FQtrNo
)

, Get_PDP_Visits_Combined AS	
(
select									
dc.calendardate,
dc.fyearperiod,
dc.fyearweek,
dc.FYearNo,
dc.FWeekNo,
dc.FPeriodNo,
dc.FQtrNo,									
'Ecommerce' Division,									
sum(cy.PDP_Views) PDP_Visits						
from									
`mattress-firm-inc.mfrm_sales.dim_calendar` dc									
left join Get_PDP_Visits_Agg as cy on dc.CalendarDate = cy.CalendarDate									
where
 									
dc.FYearno >= 2018									
group by
dc.fyearperiod,
dc.fyearweek,
dc.CalendarDate,
dc.FYearNo,
dc.FWeekNo,
dc.FPeriodNo,
dc.FQtrNo
)
Select * FROM Get_PDP_Visits_Combined