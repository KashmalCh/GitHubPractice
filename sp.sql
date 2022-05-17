CREATE OR REPLACE PROCEDURE `prod-mattressfinder-project.mattressfinder_data.sp_load_recommendation_data_variant`()
BEGIN
/*
############## V1.0 ###########################################################################
Date of Change: 11/01/2021, User Story : 47573, Developer: Usama Amjad, Reviewed By : Rehan Niaz
Description: SP loads the MM data from stage to main table on the basis of Full load or CDC
Version: Initial Version
###############################################################################################
############## V1.1 ###########################################################################
Date of Change: 03/01/2022, User Story : , Developer: Usama Amjad, Reviewed By : Rehan Niaz
Change : Applying following New changes
			1. Added new columns D365ProductID, D365RecordId
###############################################################################################
*/

	DECLARE StatusCheck string DEFAULT NULL;
-- variables
declare startdatetime timestamp default timestamp(cast(current_timestamp() as datetime), 'America/Chicago');
declare rowCount int64;
declare message string; -- message for logging table
declare scriptjob_id string;
declare StageMode string;

set StatusCheck = (select mode from `mattress-firm-inc.mfrm_config_logging_data.jobs_config_data` where ConfigName = 'RecommenderLogMainRecommendations');
set StageMode = (select Mode from `mattress-firm-inc.mfrm_config_logging_data.jobs_config_data` where ConfigName = 'RecommenderLog');

			   -- Logging
    set rowCount = (select @@row_count);
    set scriptjob_id = (SELECT @@script.job_id);
    set message = CONCAT('Before "2021-03-30T00:00:00.00" - ',scriptjob_id);
    IF StatusCheck = 'FullLoad' THEN
    INSERT INTO `mattress-firm-inc.mfrm_config_logging_data.logging_data`
    ( FunctionName, ProcessedRows, FailedRows, StartDateTime, EndDateTime, Status, TargetTable, Message, JobType, Source, LogID)
    VALUES ('sp_load_recommendation_data_variant', rowCount, 0, cast(startdatetime as string),cast(CURRENT_DATETIME('America/Chicago') as string), 'Success',
    'mattressfinder_data.mf_recommendations_data_variant',message,StatusCheck,'mattressfinder_staging_data.mf_recommender_log_stg', CAST(FLOOR(1000*RAND()) AS STRING) || '-' || cast(CURRENT_DATETIME('America/Chicago') as string));
    END IF;

	INSERT INTO
	  `prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` ( RecommenderLogId,
		RecommendationOrder,
		RecommendationItem,
		RecommenderationRank,
		MattressSize,
		VariantId,
		OLPPrice,
		SalesPrice,
		LastModifiedOn,
		D365ProductID,
	  	D365RecordId,
		CreatedBy,
		ModifiedBy,
		CreatedDatetime,
		ModifiedDatetime )
	SELECT
	  DISTINCT RecommenderLogId,
	  RecommendationOrder,
	  RecommendationItem,
	  RecommenderationRank,
	  CAST(json_extract_scalar(var,
		  '$.size') AS STRING) AS MattressSize,
	  CAST(json_extract_scalar(var,
		  '$.variantId') AS STRING) AS VariantId,
	  CAST(json_extract_scalar(var,
		  '$.olpPrice') AS NUMERIC) AS OLPPrice,
	  CAST(json_extract_scalar(var,
		  '$.salesPrice') AS NUMERIC) AS SalesPrice,
	  LastModifiedOn,
	  D365ProductID,
	  CAST(json_extract_scalar(var,
			'$.d365RecordId') AS STRING) AS D365RecordId,
	  'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' AS CreatedBy,
	  'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' AS ModifiedBy,
	  CURRENT_DATETIME('America/Chicago') AS CreatedDatetime,
	  CURRENT_DATETIME('America/Chicago') AS ModifiedDatetime
	FROM (
	  WITH
		json_array_table AS (
		SELECT
		  RecommenderLogId,
		  LastModifiedOn,
		  json_text AS a,
		  CustomerResponses
		FROM
		  `prod-mattressfinder-project.mattressfinder_staging_data.mf_recommender_log_stg` rlstg3
		LEFT JOIN
		  UNNEST(COALESCE((JSON_EXTRACT_ARRAY(JSON_EXTRACT(ResponsePayload,
				'$.RecommendedProduct'))),(JSON_EXTRACT_ARRAY(JSON_EXTRACT(ResponsePayload,
				'$.recommendedProduct'))))) AS json_text
		WHERE
		  CAST(rlstg3.LastModifiedDate AS DATE)>CAST('2021-03-30' AS DATE) ),
		json_array_element_table AS (
		SELECT
		  RecommenderLogId,
		  LastModifiedOn,
		  a,
		  CustomerResponses,
		  ROW_NUMBER() OVER (PARTITION BY RecommenderLogId,LOWER(COALESCE(CAST(json_extract_scalar(a,
        '$.IsAvailableToTryInStore') AS STRING),CAST(json_extract_scalar(a,
        '$.isAvailableToTryInStore') AS STRING))) ) AS array_idx
		FROM
		  json_array_table )
	  SELECT
		DISTINCT CAST(json_EXTRACT(a,
			'$.VariantId') AS STRING) AS VariantId,
		RecommenderLogId,
		array_idx AS RecommendationOrder,
		COALESCE(COALESCE(COALESCE(json_extract_scalar(a,
		  '$.Sku'),json_extract_scalar(a,
		  '$.SKU') ),json_extract_scalar(a,
		  '$.sku')),json_extract_scalar(a,
		  '$.AX2012ProductID')) AS RecommendationItem,
		LastModifiedOn AS LastModifiedOn,
		json_extract_scalar(a,
		  '$.Rank') AS RecommenderationRank,
		CAST(json_extract_scalar(a,
			'$.D365ProductID') AS STRING) AS D365ProductID,
		json_extract_scalar(a,
		  '$.AX2012ProductID') AS AX2012ProductID,
		COALESCE(COALESCE(json_extract_scalar(a,
		  '$.Sku'),json_extract_scalar(a,
		  '$.SKU') ),json_extract_scalar(a,
		  '$.sku')) AS Sku
	  FROM
		json_array_element_table)A
	LEFT JOIN
	  UNNEST(JSON_EXTRACT_ARRAY(VariantId)) var
	WHERE
	  RecommenderLogId NOT IN (
	  SELECT
		DISTINCT recItems.RecommenderLogId
	  FROM
		`prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` recItems)
	ORDER BY
	  1,
	  2 ;
			   -- Logging
		set rowCount = (select @@row_count);
		  set scriptjob_id = (SELECT @@script.job_id);
		set message = CONCAT('After "2021-03-30T00:00:00.00" - ',scriptjob_id);
		
		INSERT INTO `mattress-firm-inc.mfrm_config_logging_data.logging_data`
		( FunctionName, ProcessedRows, FailedRows, StartDateTime, EndDateTime, Status, TargetTable, Message, JobType, Source, LogID)
		VALUES ('sp_load_recommendation_data_variant', rowCount, 0, cast(startdatetime as string),cast(CURRENT_DATETIME('America/Chicago') as string), 'Success',
		'mattressfinder_data.mf_recommendations_data_variant',message,StatusCheck,'mattressfinder_staging_data.mf_recommender_log_stg', CAST(FLOOR(1000*RAND()) AS STRING) || '-' || cast(CURRENT_DATETIME('America/Chicago') as string));

--End part in which FullLoad data will populate
  
-------------------------------------------------------------------------------------------------
--Starting the CDC Load

ELSEIF StatusCheck = 'CDC'
THEN


	DROP TABLE IF EXISTS _SESSION.RecommendationsDataVariantTemp;
	CREATE TEMP TABLE RecommendationsDataVariantTemp AS
	SELECT
	  DISTINCT RecommenderLogId,
	  RecommendationOrder,
	  RecommendationItem,
	  RecommenderationRank,
	  CAST(json_extract_scalar(var,
		  '$.size') AS STRING) AS MattressSize,
	  CAST(json_extract_scalar(var,
		  '$.variantId') AS STRING) AS VariantId,
	  CAST(json_extract_scalar(var,
		  '$.olpPrice') AS NUMERIC) AS OLPPrice,
	  CAST(json_extract_scalar(var,
		  '$.salesPrice') AS NUMERIC) AS SalesPrice,
	  LastModifiedOn,
	  D365ProductID,
	  AX2012ProductID,
	  Sku,
	  CAST(json_extract_scalar(var,
			'$.d365RecordId') AS STRING) AS D365RecordId,
	  'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' AS CreatedBy,
	  'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' AS ModifiedBy,
	  CURRENT_DATETIME('America/Chicago') AS CreatedDatetime,
	  CURRENT_DATETIME('America/Chicago') AS ModifiedDatetime
	FROM (
	  WITH
		json_array_table AS (
		SELECT
		  RecommenderLogId,
		  LastModifiedOn,
		  json_text AS a,
		  CustomerResponses
		FROM
		  `prod-mattressfinder-project.mattressfinder_staging_data.mf_recommender_log_cdc_data_stg` rlstg3
		LEFT JOIN
		  UNNEST(COALESCE((JSON_EXTRACT_ARRAY(JSON_EXTRACT(ResponsePayload,
				'$.RecommendedProduct'))),(JSON_EXTRACT_ARRAY(JSON_EXTRACT(ResponsePayload,
				'$.recommendedProduct'))))) AS json_text
		WHERE
		  CAST(rlstg3.LastModifiedDate AS DATE)>CAST('2021-03-30' AS DATE) ),
		json_array_element_table AS (
		SELECT
		  RecommenderLogId,
		  LastModifiedOn,
		  a,
		  CustomerResponses,
		  ROW_NUMBER() OVER (PARTITION BY RecommenderLogId,LOWER(COALESCE(CAST(json_extract_scalar(a,
        '$.IsAvailableToTryInStore') AS STRING),CAST(json_extract_scalar(a,
        '$.isAvailableToTryInStore') AS STRING))) ) AS array_idx
		FROM
		  json_array_table )
	  SELECT
		DISTINCT CAST(json_EXTRACT(a,
			'$.VariantId') AS STRING) AS VariantId,
		RecommenderLogId,
		array_idx AS RecommendationOrder,
		COALESCE(COALESCE(COALESCE(json_extract_scalar(a,
		  '$.Sku'),json_extract_scalar(a,
		  '$.SKU') ),json_extract_scalar(a,
		  '$.sku')),json_extract_scalar(a,
		  '$.AX2012ProductID')) AS RecommendationItem,
		LastModifiedOn AS LastModifiedOn,
		json_extract_scalar(a,
		  '$.Rank') AS RecommenderationRank,
		CAST(json_extract_scalar(a,
			'$.D365ProductID') AS STRING) AS D365ProductID,
		json_extract_scalar(a,
		  '$.AX2012ProductID') AS AX2012ProductID,
		COALESCE(COALESCE(json_extract_scalar(a,
		  '$.Sku'),json_extract_scalar(a,
		  '$.SKU') ),json_extract_scalar(a,
		  '$.sku')) AS Sku
	  FROM
		json_array_element_table)A
	LEFT JOIN
	  UNNEST(JSON_EXTRACT_ARRAY(VariantId)) var
	WHERE
	  RecommenderLogId NOT IN (
	  SELECT
		DISTINCT recItems.RecommenderLogId
	  FROM
		`prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` recItems)
	ORDER BY
	  1,
	  2 ;

	DELETE FROM `prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` WHERE RecommenderLogId IN (SELECT DISTINCT RecommenderLogId FROM RecommendationsDataVariantTemp);

	INSERT INTO
	`prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` ( RecommenderLogId,
	RecommendationOrder,
	RecommendationItem,
	RecommenderationRank,
	MattressSize,
	VariantId,
	OLPPrice,
	SalesPrice,
	LastModifiedOn,
	D365ProductID,
	D365RecordId,
	CreatedBy,
	ModifiedBy,
	CreatedDatetime,
	ModifiedDatetime )
	select
	RecommenderLogId,
	RecommendationOrder,
	RecommendationItem,
	RecommenderationRank,
	MattressSize,
	VariantId,
	OLPPrice,
	SalesPrice,
	LastModifiedOn,
	D365ProductID,
	D365RecordId,
	CreatedBy,
	ModifiedBy,
	CreatedDatetime,
	ModifiedDatetime
	from RecommendationsDataVariantTemp;


   -- Logging
	set rowCount = (select @@row_count);
	set scriptjob_id = (SELECT @@script.job_id);
	set message = CONCAT('After "2021-03-30T00:00:00.00" - ',scriptjob_id);
	
	INSERT INTO `mattress-firm-inc.mfrm_config_logging_data.logging_data`
	( FunctionName, ProcessedRows, FailedRows, StartDateTime, EndDateTime, Status, TargetTable, Message, JobType, Source, LogID)
	VALUES ('sp_load_recommendation_data_variant', rowCount, 0, cast(startdatetime as string),cast(CURRENT_DATETIME('America/Chicago') as string), 'Success',
	'mattressfinder_data.mf_recommendations_data_variant',message,StatusCheck,'mattressfinder_staging_data.mf_recommender_log_stg', CAST(FLOOR(1000*RAND()) AS STRING) || '-' || cast(CURRENT_DATETIME('America/Chicago') as string));

 
     END IF;

  -- updating start and end date in jobs config table
  UPDATE `mattress-firm-inc.mfrm_config_logging_data.jobs_config_data`
  SET 
    StartDate = startdatetime,  
    EndDate = TIMESTAMP(CAST(CURRENT_TIMESTAMP() AS DATETIME) ,'America/Chicago'),
    LastExecutionTime = (select cast(max(LastModifiedOn) as timestamp) from `prod-mattressfinder-project.mattressfinder_data.mf_recommender_log`),
    Key = (select cast(max(RecommenderLogId) as string) from `prod-mattressfinder-project.mattressfinder_data.mf_recommender_log`)
    
  WHERE ConfigName = 'RecommenderLogMainRecommendations';
    

 END;
