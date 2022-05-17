{{config(
    materialized = 'table',
    schema = 'stg'
)}}


WITH Demand_Actual_Google_Demand_Forecast_Actual_To_Forecast AS
(
Select
a.StartDate, 
a.HistoricalSearches AS Demand_Actual,
a.ForecastSearches AS Google_Demand_Forecast,
((SAFE_DIVIDE(a.HistoricalSearches, a.ForecastSearches)) - 1)  * 100 as Actual_To_Forecast,
b.fyearperiod,
b.fyearweek,
b.CalendarDate,
b.FYearNo,
b.FWeekNo,
b.FPeriodNo,
b.FQtrNo,
from 
mattress-firm-inc.mfrm_sales.us_mattress_demand_forecast a
LEFT join `mattress-firm-inc.mfrm_sales.dim_calendar` b
on a.StartDate = b.CalendarDate
Group by 
b.fyearperiod,
b.fyearweek,
b.CalendarDate,
b.FYearNo,
b.FWeekNo,
b.FPeriodNo,
b.FQtrNo,
a.StartDate,
a.ForecastSearches, 
a.HistoricalSearches
)
Select * FROM Demand_Actual_Google_Demand_Forecast_Actual_To_Forecast