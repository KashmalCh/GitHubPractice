{{config(
    materialized = 'table'
)}}

SELECT * EXCEPT(Rank)
FROM(
	SELECT*
		,ROW_NUMBER()OVER(PARTITION BY StoreId,Element,Date ORDER BY final_result.RANK ) AS StationRank
	FROM {{ref('store_weather_data_store_weather_data_stg')}} AS final_result)
	WHERE StationRank = 1