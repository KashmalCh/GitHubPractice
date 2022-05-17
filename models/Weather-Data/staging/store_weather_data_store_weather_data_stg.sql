{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT
	CTE.StoreId
	,CTE.StoreGeoLocation
	,CTE.StationId
	,CTE.StationGeoLocation
	,CTE.DistanceInMeters
	,CTE.DistanceInMiles
	,CTE.Rank
	,ghcnd.Date
	,ghcnd_inv.Firstyear --When first record entered
	,ghcnd_inv.Lastyear --When last record entered
	,ghcnd.Element
	,wded.description AS ElementDescription
	,ghcnd.Value
	,ghcnd.Mflag
	,wdfd_b.Description AS MflagDescription
	,ghcnd.Qflag
	,wdfd_a.Description AS QflagDescription
	,ghcnd.Sflag
	,wdfd_c.Description AS SflagDescription
FROM 
	{{ref('store_weather_data_nearest_station_stg')}} as CTE
LEFT JOIN(
		SELECT 
			*
		FROM 
			`bigquery-public-data.ghcn_d.ghcnd_*` ghcnd
		WHERE 
			_TABLE_SUFFIX >= '2019'
			AND 
				REGEXP_CONTAINS(_TABLE_SUFFIX, '^[1-9]')
			AND 
				ghcnd.Date >= "2019-01-01") ghcnd   --Weather data by year
		ON 
			CTE.StationId = ghcnd.Id
LEFT JOIN 
	`bigquery-public-data.ghcn_d.ghcnd_inventory` ghcnd_inv --Weather data inventory
	ON 
		CTE.StationId = ghcnd_inv.Id
	AND 
		ghcnd.element = ghcnd_inv.element
LEFT JOIN 
	{{source('dev_mfrm_sales', 'weather_data_element_description')}} wded
	ON 
		ghcnd.element = wded.element
LEFT JOIN 
	{{source('dev_mfrm_sales', 'weather_data_flag_description')}} wdfd_a
	ON 
		ghcnd.qflag = wdfd_a.value
	AND 
		wdfd_a.flag = "qflag"
LEFT JOIN 
	{{source('dev_mfrm_sales', 'weather_data_flag_description')}} wdfd_b
	ON 
		ghcnd.mflag = wdfd_b.value
	AND wdfd_b.flag = "mflag"
LEFT JOIN 
	{{source('dev_mfrm_sales', 'weather_data_flag_description')}} wdfd_c
	ON 
		ghcnd.sflag = wdfd_c.value
	AND 
		wdfd_c.flag = "sflag"
WHERE 
	ghcnd.Date IS NOT NULL
ORDER BY 
	StoreID