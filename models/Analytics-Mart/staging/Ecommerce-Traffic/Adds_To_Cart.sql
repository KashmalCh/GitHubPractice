{{config(
    materialized = 'table',
    schema = 'stg'
)}}

WITH Adds_To_Cart AS
(
	SELECT
	    dc.fyearperiod,
	    dc.fyearweek,
	    dc.CalendarDate,
	    dc.FYearNo ,
	    dc.FWeekNo ,
	    dc.FPeriodNo ,
	    dc.FQtrNo,
	    'Ecommerce' division_group,
	    SUM(sessions) Adds_To_Cart
	FROM(
	SELECT
	    date,
	    fullVisitorId,
	    count(distinct visitid) as sessions,
	    FROM
	     `mattress-firm-inc.8460582.ga_sessions_*`ga,
	      UNNEST(GA.hits) AS hits
	    WHERE 
	    _TABLE_SUFFIX >= '20180101'
	    AND regexp_contains(hits.eventInfo.eventAction,'add to cart|Product Detail Column - Add to Cart')
	    GROUP BY  date,fullVisitorId
	) AS ga
	JOIN `mattress-firm-inc.mfrm_sales.dim_calendar` dc on PARSE_DATE("%Y%m%d", ga.date) = dc.CalendarDate
	GROUP BY  dc.fyearperiod,
		  dc.fyearweek,
	    dc.CalendarDate,
	    dc.FYearNo ,
	    dc.FWeekNo ,
	    dc.FPeriodNo ,
	    dc.FQtrNo
)
Select * FROM Adds_To_Cart
