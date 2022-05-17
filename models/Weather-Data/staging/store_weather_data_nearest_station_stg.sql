{{config(
    materialized = 'table',
    schema = 'stg'
)}}

SELECT *
	,nearest_stations.DistanceInMeters * 0.000621 AS DistanceInMiles
FROM (
	SELECT
		sm.StoreID
		,ST_GEOGPOINT(CAST(sd.longitude AS float64),CAST(sd.latitude AS float64)) AS StoreGeoLocation
		,sd.Id AS StationId
		,ST_GEOGPOINT(CAST(sm.longitude AS float64),CAST(sm.latitude AS float64)) AS StationGeoLocation
		,ST_DISTANCE(ST_GEOGPOINT(CAST(sd.longitude AS float64),CAST(sd.latitude AS float64)),ST_GEOGPOINT(CAST(sm.longitude AS float64),CAST(sm.latitude AS float64))) AS DistanceInMeters
		,ROW_NUMBER() OVER(PARTITION BY sm.StoreId ORDER BY ST_DISTANCE(ST_GEOGPOINT(CAST(sd.longitude AS float64),CAST(sd.latitude AS float64)),ST_GEOGPOINT(CAST(sm.longitude AS float64),CAST(sm.latitude AS float64)))) AS Rank
	FROM
		{{source('dev_mfrm_sales', 'store_master')}} sm
	CROSS JOIN 
		`bigquery-public-data.ghcn_d.ghcnd_stations` sd
	WHERE
		sm.StoreID != "759001" --Longitude of this Id causing some error
	AND 
		sm.StoreStatus = 'O'
	AND 
		LOWER(sm.Type) = 'retail'
	AND 
		sm.Latitude IS NOT NULL
	AND 
		sm.Longitude IS NOT NULL
	AND 
		sd.Latitude IS NOT NULL
	AND 
		sd.Longitude IS NOT NULL) nearest_stations
	WHERE 
		nearest_stations.Rank <= 15