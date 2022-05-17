{{config(
    materialized = 'table',
    schema = 'stg'
)}}

WITH Earned_Traffic AS
(
SELECT
 
    fyearperiod,
	fyearweek,
    CalendarDate,
    FYearNo AS FYear,
    FWeekNo AS FWeek,
    FPeriodNo AS FPeriod,
    FQtrNo AS FQtr, 
    'Ecommerce' division_group,
    session_count earned_session_count
FROM (
SELECT
    fyearperiod,
	fyearweek,
    CalendarDate,
    FYearNo,
    FWeekNo,
    FPeriodNo,
    FQtrNo, 
    CASE
WHEN trafficSource.Medium="affiliate" THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Campaign,"^(.*display_dynamic.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Campaign,"^(.*dynamicremarketing.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Campaign,"^(.*remarketing.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Campaign,"^(.*Display.*|.*display.*)$") THEN "Paid"
WHEN (trafficSource.Medium="cpc" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*gdn.*)$")) THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Medium,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Source,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") THEN "Paid"
WHEN (trafficSource.Medium="paidsocial" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*junksleep.*|.*Awareness.*)$")) THEN "Earned"
WHEN trafficSource.Campaign="26579484" THEN "Paid"
WHEN trafficSource.Campaign="26578107" THEN "Paid"
WHEN (trafficSource.Medium="social" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*_up.*|.*_uf.*|.*junksleep.*|.*awareness.*|.*Awareness.*)$")) THEN "Earned"
WHEN (trafficSource.Medium="paidsocial" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*dpa.*)$")) THEN "Paid"
WHEN (trafficSource.Medium="social" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*dpa.*)$")) THEN "Paid"
WHEN (trafficSource.Medium="paidsocial" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*_lf.*|.*promo.*)$")) THEN "Paid"
WHEN (trafficSource.Medium="social" OR REGEXP_CONTAINS(trafficSource.Medium,"^(.*social.*|.*facebook.*|.*instagram.*|.*Social.*|.*Facebook.*|.*Instagram.*|.*pinterest.*)$")) THEN "Earned"
WHEN (trafficSource.Medium="social" OR REGEXP_CONTAINS(trafficSource.Source,"^(.*social.*|.*facebook.*|.*instagram.*|.*Social.*|.*Facebook.*|.*Instagram.*|.*pinterest.*)$")) THEN "Earned"
WHEN (trafficSource.Medium="cpc" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*local.*)$")) THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Source,"^(.*yelp.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Medium,"^(.*yelp.*)$") THEN "Paid"
WHEN trafficSource.Medium="email" THEN "Earned"
WHEN REGEXP_CONTAINS(trafficSource.Source,"^(.*auto_trans.*)$") THEN "Earned"
WHEN (trafficSource.Medium="email" and REGEXP_CONTAINS(trafficSource.Source,"^(.*auto.*)$")) THEN "Earned"
WHEN (trafficSource.Medium="email" and REGEXP_CONTAINS(trafficSource.Source,"^(.*promo.*)$")) THEN "Earned"
WHEN trafficSource.Medium="organic" THEN "Earned"
WHEN trafficSource.Source="organic" THEN "Earned"
WHEN (trafficSource.Medium="cpc" and REGEXP_CONTAINS(trafficSource.Campaign, "^(.*PLA.*|.*pla.*|.*shopping.*)$")) THEN "Paid"
WHEN (trafficSource.Medium="cpc" and REGEXP_CONTAINS(trafficSource.Campaign, "^(.*nb_.*|.*NB_.*|.*non-brand.*|.*ssb.*)$")) THEN "Paid"
WHEN (trafficSource.Medium="cpc" and REGEXP_CONTAINS(trafficSource.Campaign,"^(.*LIA.*|.*lia.*|.*lias.*)$")) THEN "Paid"
WHEN (trafficSource.Medium="cpc" and REGEXP_CONTAINS(trafficSource.Campaign, "^(.*Brand_.*|.*Local -.*|.*brand_.*|.*local -.*)$")) THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Medium,"^(.*cpc.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Source,"^(.*cpc.*)$") THEN "Paid"
WHEN trafficSource.Source="google my business" THEN "Earned"
WHEN trafficSource.Medium="referral" THEN "Earned"
WHEN trafficSource.Source="(direct)" THEN "Earned"
WHEN trafficSource.Medium="video" THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Medium,"^(.*audio.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Source,"^(.*audio.*)$") THEN "Paid"
WHEN REGEXP_CONTAINS(trafficSource.Medium,"^(.*video.*|.*youtube.*)$") THEN "Paid"
WHEN trafficSource.Campaign="xyz" THEN "Paid"
else "Earned"
END AS Paid_vs_Earned,
    count(distinct concat(clientId,visitId)) session_count                                                  
FROM
  `mattress-firm-inc.8460582.ga_sessions_*`,
  UNNEST(hits) hits
  join `mattress-firm-inc.mfrm_sales.dim_calendar` dc on PARSE_DATE("%Y%m%d", date) = dc.CalendarDate
WHERE
  _TABLE_SUFFIX >= '20180101'
  and FYearNo >= 2018

   and not regexp_contains(trafficSource.campaign,'awareness')
    and hits.page.hostname not like '%dev%'
    and hits.page.hostname not like '%test%'
    and hits.page.pagePath not like '%whoops%'
    and hits.page.hostname is not null
    and not regexp_contains(hits.page.pagePath,'synthetictest|001038')
    --and hits.page.hostname not like '%dynamics.com%'
    and hits.page.hostname not like '%dynamic%' --New Added
    and hits.page.hostname not like '%not set%' --New Added
    and hits.page.hostname not like '%community%'--New Added
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
    FQtrNo, 
    Paid_vs_Earned
)
    where Paid_vs_Earned = 'Earned'
)
Select * FROM Earned_Traffic