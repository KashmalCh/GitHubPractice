{{config(
    materialized = 'table',
    schema = 'stg'
)}}


WITH Spent_Calculation AS
(
select 
'Ecommerce' division_group,
sum(ab.cost)/1000000 spent,
dc.fyearweek,
dc.CalendarDate,
dc.FYearNo AS FYear,
dc.FWeekNo AS FWeek,
dc.FPeriodNo AS FPeriod,
dc.FQtrNo AS FQtr, 
from `mattress-firm-inc.mfrm_google_ads_dataset_2139348461.CampaignBasicStats_2139348461` ab
	join `mattress-firm-inc.mfrm_sales.dim_calendar` dc on dc.CalendarDate = ab.Date
where 
	dc.fyearno >= 2018
	and ExternalCustomerId = 1344991843
group by 
	dc.fyearweek,
	dc.CalendarDate,
	dc.FYearNo,
	dc.FWeekNo,
	dc.FPeriodNo,
	dc.FQtrNo
)

,Segment_Calculation AS
(
Select
a.CalendarDate,
a.fyearweek,
a.FYearNo,
a.FWeekNo,
a.FPeriodNo, 
a.FQtrNo,
a.Segment,
'Ecommerce' division_group
from

(Select distinct fyearweek,
CalendarDate,
FYearNo,
FWeekNo,
FPeriodNo, 
FQtrNo ,pp.Segment
from
(select distinct 
fyearweek,
CalendarDate,
FYearNo,
FWeekNo,
FPeriodNo, 
FQtrNo   
from `mattress-firm-inc.mfrm_sales.dim_calendar`
where 
	fyearno >= 2018
) dc
CROSS JOIN 
(select distinct Segment
FROM `mattress-firm-inc.mfrm_reporting_dataset.rpt_ga_purchase_propensity`) pp
) a
)

select
a.CalendarDate,
a.fyearweek,
a.FYearNo,
a.FWeekNo,
a.FPeriodNo, 
a.FQtrNo,   
SAFE_DIVIDE(sum(b.spent) , count(distinct a.segment)) Paid_Search_Spent
FROM 
Spent_Calculation b
INNER JOIN 
Segment_Calculation a
on a.CalendarDate = b.CalendarDate
group by 
a.CalendarDate,
a.fyearweek,
a.FYearNo,
a.FWeekNo,
a.FPeriodNo, 
a.FQtrNo