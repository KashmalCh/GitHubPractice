{{config(
    materialized = 'table'
)}}

SELECT
DC.CalendarDate AS Date,
DC.FYearNo AS Fiscal_Year,
row_number() OVER (ORDER BY C.CalendarDate, C.SnapshotWrittenWeek,C.FYear,C.FWeek,C.FPeriod,C.FQtr) AS Sequence,
DC.FQtrNo AS Fiscal_Quarter,
DC.FPeriodNo AS Fiscal_Period,
DC.FWeekNo AS Fiscal_Week,
Q.Chat_Rev AS Cart_Rev,
Q.Chat_Margin AS Cart_Margin,
Q.Chat_Trans AS Cart_Trans,
C.Chat_Rev AS Chat_Rev,
C.Chat_Margin AS Chat_Margin,
C.Chat_Trans AS Chat_Trans,
P.Chat_Rev AS Phone_Rev,
P.Chat_Margin AS Phone_Margin,
P.Chat_Trans AS Phone_Trans,
S.Chat_Rev AS Store_Rev,
S.Chat_Margin AS Store_Margin,
S.Chat_Trans AS Store_Trans,
(Q.Chat_Rev + P.Chat_Rev + C.Chat_Rev) AS Ecomm_Rev,
(Q.Chat_Margin + P.Chat_Margin + C.Chat_Margin) AS Ecomm_Margin,
(Q.Chat_Trans + P.Chat_Trans + C.Chat_Trans) AS Ecomm_Trans,
PS.Paid_Search_Spent,
ALS.All_Sessions,
NAS.Non_Awareness_Sessions,
SS.Shop_Sessions,
SA.Store_Sessions,
ET.earned_session_count,
PT.paid_session_count,
AC.Adds_To_Cart,
PDP.PDP_Visits,
GDP.Google_Demand_Forecast,
GDP.Demand_Actual,
GDP.Actual_To_Forecast,
GCPC.Google_CPC,
(PS.Paid_Search_Spent / SS.Shop_Sessions) as Shopping_CPC,
SUBSTRING(FORMAT_DATE('%A',DC.CalendarDate),1,3) AS Day_Of_Week_No, 
CASE
    WHEN FORMAT_DATE('%A', DC.CalendarDate) = 'Wednesday' THEN 1
    WHEN FORMAT_DATE('%A', DC.CalendarDate) = 'Thursday' THEN 2
    WHEN FORMAT_DATE('%A', DC.CalendarDate) = 'Friday' THEN 3
    WHEN FORMAT_DATE('%A', DC.CalendarDate) = 'Saturday' THEN 4
    WHEN FORMAT_DATE('%A', DC.CalendarDate) = 'Sunday' THEN 5
    WHEN FORMAT_DATE('%A', DC.CalendarDate) = 'Monday' THEN 6
    WHEN FORMAT_DATE('%A', DC.CalendarDate) = 'Tuesday' THEN 7
    ELSE 0
END AS Week_Day

FROM 
`mattress-firm-inc.mfrm_sales.dim_calendar` DC
LEFT JOIN
{{ref('Get_Chat_Level_Data')}}  AS C
ON DC.CalendarDate = C.CalendarDate
LEFT JOIN 
{{ref('Get_Phone_Level_Data')}} AS P 
ON DC.CalendarDate = P.CalendarDate
LEFT JOIN 
{{ref('Get_Store_Level_Data')}} AS S 
ON DC.CalendarDate = S.CalendarDate
LEFT JOIN
{{ref('Get_Cart_Level_Data')}} AS Q 
ON DC.CalendarDate = Q.CalendarDate
LEFT JOIN 
{{ref('Paid_Search_Spent_Calculation')}} AS PS 
ON DC.CalendarDate = PS.CalendarDate
LEFT JOIN 
{{ref('All_Sessions')}} AS ALS 
ON DC.CalendarDate = ALS.CalendarDate
LEFT JOIN 
{{ref('Non_Awareness_Sessions')}} AS NAS 
ON DC.CalendarDate = NAS.CalendarDate
LEFT JOIN 
{{ref('Shop_Sessions')}} AS SS 
ON DC.CalendarDate = SS.CalendarDate
LEFT JOIN 
{{ref('Store_Actions')}} AS SA
ON DC.CalendarDate = SA.CalendarDate
LEFT JOIN
{{ref('Earned_Traffic')}} AS ET
ON DC.CalendarDate = ET.CalendarDate
LEFT JOIN
{{ref('Paid_Traffic')}} AS PT
ON DC.CalendarDate = PT.CalendarDate
LEFT JOIN 
{{ref('Adds_To_Cart')}} AS AC 
ON DC.CalendarDate = AC.CalendarDate
LEFT JOIN
{{ref('Get_PDP_Visits_Combined')}} AS PDP 
ON DC.CalendarDate = PDP.CalendarDate
LEFT JOIN
{{ref('Demand_Actual_Google_Demand_Forecast_Actual_To_Forecast')}} AS GDP 
ON DC.CalendarDate = GDP.CalendarDate
LEFT JOIN
{{ref('Google_CPC')}} AS GCPC 
ON DC.CalendarDate = GCPC.CalendarDate
WHERE DC.CalendarDate >= '2018-10-02'
and DC.CalendarDate <= current_date()
