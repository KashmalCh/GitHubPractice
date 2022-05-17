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
  DECLARE startdatetime TIMESTAMP default TIMESTAMP(CAST(CURRENT_TIMESTAMP() AS DATETIME),'America/Chicago');
  DECLARE rowCount INT64;
  DECLARE message String; -- message for logging table
  DECLARE scriptjob_id String;
  DECLARE StageMode String ;
  
  SET StatusCheck =(SELECT mode FROM `mattress-firm-inc.mfrm_config_logging_data.jobs_config_data` WHERE ConfigName = 'RecommenderLogMainRecommendations');
  SET StageMode =(SELECT Mode FROM `mattress-firm-inc.mfrm_config_logging_data.jobs_config_data` where ConfigName = 'RecommenderLog');
  
  -- INCASE OF FULL LOAD ONLY
  IF StatusCheck = 'FullLoad' or StageMode = 'FullLoad'
  THEN
    TRUNCATE TABLE  `prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant`;

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
		CreatedBy,
		ModifiedBy,
		CreatedDatetime,
		ModifiedDatetime )
	SELECT DISTINCT
	  A.RecommenderLogId,
	  A.RecommendationOrder,
	  A.RecommendationItem,
	  A.RecommenderationRank,
	  Price.Size AS Size,
	  Price.Variant AS VariantId,
	  Price.OLPPrice AS OLPPrice,
	  Price.SalesPrice AS SalesPrice,
	  LastModifiedOn,
	  "DAG - load-recommenderlog-data-to-bq-v1.0" AS CreatedBy,
	  "DAG - load-recommenderlog-data-to-bq-v1.0" AS ModifiedBy,
	  CAST(CURRENT_DATETIME('America/Chicago') AS datetime) AS CreatedDatetime,
	  CAST(CURRENT_DATETIME('America/Chicago') AS datetime) AS ModifiedDatetime
	FROM(
	SELECT
	  A.RecommenderLogId,
	  A.RecommendationOrder,
	  A.RecommendationItem,
	  A.RecommenderationRank,
	  [STRUCT <Size STRING, OLPPrice NUMERIC, SalesPrice NUMERIC, Variant STRING>
	  ('queen',A.QueenOLPPrice,A.QueenSalesPrice,A.VariantId),
	  ('twin',A.TwinOLPPrice,A.TwinSalesPrice,A.VariantId),
	  ('twin xl',A.TwinXLOLPPrice,A.TwinXLSalesPrice,A.VariantId),
	  ('full',A.FullOLPPrice,A.FullSalesPrice,A.VariantId),
	  ('king',A.KingOLPPrice,A.KingSalesPrice,A.VariantId),
	  ('calkingprice',A.CalKingOLPPrice,A.CalKingSalesPrice,A.VariantId)] AS Prices,
	  LastModifiedOn
	FROM (
	  WITH
		json_array_table AS (
		SELECT
		  RecommenderLogId,
		  LastModifiedOn,
		  json_text AS a
		FROM
		  `prod-mattressfinder-project.mattressfinder_staging_data.mf_recommender_log_stg` rlstg3
		LEFT JOIN
		  UNNEST(COALESCE((JSON_EXTRACT_ARRAY(JSON_EXTRACT(ResponsePayload,
				'$.RecommendedProduct'))),(JSON_EXTRACT_ARRAY(JSON_EXTRACT(ResponsePayload,
				'$.recommendedProduct'))))) AS json_text
		WHERE
		  CAST(rlstg3.LastModifiedDate AS DATE) <= CAST('2021-03-30' AS DATE) ),
		json_array_element_table AS (
		SELECT
		  RecommenderLogId,
		  LastModifiedOn,
		  a,
		  ROW_NUMBER() OVER (PARTITION BY RecommenderLogId,LOWER(COALESCE(CAST(json_extract_scalar(a,
        '$.IsAvailableToTryInStore') AS STRING),CAST(json_extract_scalar(a,
        '$.isAvailableToTryInStore') AS STRING))) ) AS array_idx
		FROM
		  json_array_table )
	  SELECT
		DISTINCT rlstg3.RecommenderLogId,
		array_idx AS RecommendationOrder,
		COALESCE(COALESCE(json_extract_scalar(a,
		  '$.Sku'),json_extract_scalar(a,
		  '$.SKU') ),json_extract_scalar(a,
		  '$.sku')) AS RecommendationItem,
		COALESCE(json_extract_scalar(a,
		  '$.Rank'),json_extract_scalar(a,
		  '$.rank')) AS RecommenderationRank,
		COALESCE(CAST(json_extract_scalar(a,
			'$.QueenOLPPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.queenOLPPrice') AS NUMERIC)) AS QueenOLPPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.TwinOLPPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.twinOLPPrice') AS NUMERIC)) AS TwinOLPPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.TwinXLOLPPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.twinXLOLPPrice') AS NUMERIC)) AS TwinXLOLPPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.FullOLPPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.fullOLPPrice') AS NUMERIC)) AS FullOLPPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.KingOLPPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.kingOLPPrice') AS NUMERIC)) AS KingOLPPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.CalKingOLPPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.calKingOLPPrice') AS NUMERIC)) AS CalKingOLPPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.QueenSalesPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.queenSalesPrice') AS NUMERIC)) AS QueenSalesPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.TwinSalesPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.twinSalesPrice') AS NUMERIC)) AS TwinSalesPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.FullSalesPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.fullSalesPrice') AS NUMERIC)) AS FullSalesPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.KingSalesPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.kingSalesPrice') AS NUMERIC)) AS KingSalesPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.CalKingSalesPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.calKingSalesPrice') AS NUMERIC)) AS CalKingSalesPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.TwinXLSalesPrice') AS NUMERIC),CAST(json_extract_scalar(a,
			'$.twinXLSalesPrice') AS NUMERIC)) AS TwinXLSalesPrice,
		COALESCE(CAST(json_extract_scalar(a,
			'$.VariantId') AS STRING),CAST(json_extract_scalar(a,
			'$.variantId') AS STRING)) AS VariantId,
		rlstg3.LastModifiedOn AS LastModifiedOn
	  FROM
		json_array_element_table AS rlstg3
	  WHERE
		rlstg3.RecommenderLogId NOT IN (
		SELECT
		  DISTINCT recItems.RecommenderLogId
		FROM
		  `prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` recItems)
	  ORDER BY
		array_idx,
		rlstg3.RecommenderLogId )A
	ORDER BY
	  1)A
	LEFT JOIN UNNEST(Prices) AS Price
	ORDER BY 1,2
	;

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
