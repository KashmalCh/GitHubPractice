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

-- INCASE OF FULL LOAD ONLY
IF StatusCheck = 'FullLoad' or StageMode = 'FullLoad'
	THEN
		TRUNCATE TABLE `prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant`;

insert into
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
select distinct
	A.RecommenderLogId,
	A.RecommendationOrder,
	A.RecommendationItem,
	A.RecommenderationRank,
	Price.Size as Size,
	Price.Variant as VariantId,
	Price.OLPPrice as OLPPrice,
	Price.SalesPrice as SalesPrice,
	LastModifiedOn,
	'DAG - load-recommenderlog-data-to-bq-v1.0' as CreatedBy,
	'DAG - load-recommenderlog-data-to-bq-v1.0' as ModifiedBy,
	cast(current_datetime('America/Chicago') as datetime) as CreatedDatetime,
	cast(current_datetime('America/Chicago') as datetime) as ModifiedDatetime
from(
	select
		A.RecommenderLogId,
		A.RecommendationOrder,
		A.RecommendationItem,
		A.RecommenderationRank,
		[struct <Size string, OLPPrice numeric, SalesPrice numeric, Variant string>
			('queen', A.QueenOLPPrice, A.QueenSalesPrice, A.VariantId),
			('twin', A.TwinOLPPrice, A.TwinSalesPrice, A.VariantId),
			('twin xl', A.TwinXLOLPPrice, A.TwinXLSalesPrice, A.VariantId),
			('full', A.FullOLPPrice, A.FullSalesPrice, A.VariantId),
			('king', A.KingOLPPrice, A.KingSalesPrice, A.VariantId),
			('calkingprice', A.CalKingOLPPrice, A.CalKingSalesPrice, A.VariantId)] as Prices,
		LastModifiedOn
	from (
		with
		json_array_table as (
			select
				RecommenderLogId,
				LastModifiedOn,
				json_text as a
			from
				`prod-mattressfinder-project.mattressfinder_staging_data.mf_recommender_log_stg` rlstg3
			left join
				unnest(coalesce((json_extract_array(json_extract(ResponsePayload,
					'$.RecommendedProduct'))), (json_extract_array(json_extract(ResponsePayload,
					'$.recommendedProduct'))))) as json_text
			where
				cast(rlstg3.LastModifiedDate as date) <= cast('2021-03-30' as date)
		),

		json_array_element_table as (
			select
				RecommenderLogId,
				LastModifiedOn,
				a,
				row_number() over (partition by RecommenderLogId, lower(coalesce(cast(json_extract_scalar(a,
					'$.IsAvailableToTryInStore') as string), cast(json_extract_scalar(a,
					'$.isAvailableToTryInStore') as string))) ) as array_idx
			from
				json_array_table 
		)

		select distinct
			rlstg3.RecommenderLogId,
			array_idx as RecommendationOrder,
			coalesce(coalesce(json_extract_scalar(a,
				'$.Sku'), json_extract_scalar(a,
				'$.SKU') ), json_extract_scalar(a,
				'$.sku')) as RecommendationItem,
			coalesce(json_extract_scalar(a,
				'$.Rank'), json_extract_scalar(a,
				'$.rank')) as RecommenderationRank,
			coalesce(cast(json_extract_scalar(a,
				'$.QueenOLPPrice') as numeric), cast(json_extract_scalar(a,
				'$.queenOLPPrice') as numeric)) as QueenOLPPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.TwinOLPPrice') as numeric), cast(json_extract_scalar(a,
				'$.twinOLPPrice') as numeric)) as TwinOLPPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.TwinXLOLPPrice') as numeric), cast(json_extract_scalar(a,
				'$.twinXLOLPPrice') as numeric)) as TwinXLOLPPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.FullOLPPrice') as numeric), cast(json_extract_scalar(a,
				'$.fullOLPPrice') as numeric)) as FullOLPPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.KingOLPPrice') as numeric), cast(json_extract_scalar(a,
				'$.kingOLPPrice') as numeric)) as KingOLPPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.CalKingOLPPrice') as numeric), cast(json_extract_scalar(a,
				'$.calKingOLPPrice') as numeric)) as CalKingOLPPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.QueenSalesPrice') as numeric), cast(json_extract_scalar(a,
				'$.queenSalesPrice') as numeric)) as QueenSalesPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.TwinSalesPrice') as numeric), cast(json_extract_scalar(a,
				'$.twinSalesPrice') as numeric)) as TwinSalesPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.FullSalesPrice') as numeric), cast(json_extract_scalar(a,
				'$.fullSalesPrice') as numeric)) as FullSalesPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.KingSalesPrice') as numeric), cast(json_extract_scalar(a,
				'$.kingSalesPrice') as numeric)) as KingSalesPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.CalKingSalesPrice') as numeric), cast(json_extract_scalar(a,
				'$.calKingSalesPrice') as numeric)) as CalKingSalesPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.TwinXLSalesPrice') as numeric), cast(json_extract_scalar(a,
				'$.twinXLSalesPrice') as numeric)) as TwinXLSalesPrice,
			coalesce(cast(json_extract_scalar(a,
				'$.VariantId') as string), cast(json_extract_scalar(a,
				'$.variantId') as string)) as VariantId,
			rlstg3.LastModifiedOn as LastModifiedOn
		from
			json_array_element_table as rlstg3
		where
			rlstg3.RecommenderLogId not in (
				select distinct recItems.RecommenderLogId
				from
					`prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` recItems)
		order by
			array_idx,
			rlstg3.RecommenderLogId )A
	order by
		1)A
left join unnest(Prices) as Price
order by 1, 2;

-- Logging
set rowCount = (select @@row_count);
set scriptjob_id = (select @@script.job_id);
set message = concat('Before "2021-03-30T00:00:00.00" - ', scriptjob_id);
IF StatusCheck = 'FullLoad' THEN
		INSERT INTO `mattress-firm-inc.mfrm_config_logging_data.logging_data`
		( FunctionName, ProcessedRows, FailedRows, StartDateTime, EndDateTime, Status, TargetTable, Message, JobType, Source, LogID)
		VALUES ('sp_load_recommendation_data_variant', rowCount, 0, cast(startdatetime as string), cast(CURRENT_DATETIME('America/Chicago') as string), 'Success',
		'mattressfinder_data.mf_recommendations_data_variant', message, StatusCheck, 'mattressfinder_staging_data.mf_recommender_log_stg', CAST(FLOOR(1000*RAND()) AS STRING) || '-' || cast(CURRENT_DATETIME('America/Chicago') as string));
end IF;

insert into
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
select distinct
	RecommenderLogId,
	RecommendationOrder,
	RecommendationItem,
	RecommenderationRank,
	cast(json_extract_scalar(var,
		'$.size') as string) as MattressSize,
	cast(json_extract_scalar(var,
		'$.variantId') as string) as VariantId,
	cast(json_extract_scalar(var,
		'$.olpPrice') as numeric) as OLPPrice,
	cast(json_extract_scalar(var,
		'$.salesPrice') as numeric) as SalesPrice,
	LastModifiedOn,
	D365ProductID,
	cast(json_extract_scalar(var,
		'$.d365RecordId') as string) as D365RecordId,
	'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' as CreatedBy,
	'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' as ModifiedBy,
	current_datetime('America/Chicago') as CreatedDatetime,
	current_datetime('America/Chicago') as ModifiedDatetime
from (
	with
	json_array_table as (
		select
			RecommenderLogId,
			LastModifiedOn,
			json_text as a,
			CustomerResponses
		from
			`prod-mattressfinder-project.mattressfinder_staging_data.mf_recommender_log_stg` rlstg3
		left join
			unnest(coalesce((json_extract_array(json_extract(ResponsePayload,
				'$.RecommendedProduct'))), (json_extract_array(json_extract(ResponsePayload,
				'$.recommendedProduct'))))) as json_text
		where
			cast(rlstg3.LastModifiedDate as date) > cast('2021-03-30' as date) 
	),

	json_array_element_table as (
		select
			RecommenderLogId,
			LastModifiedOn,
			a,
			CustomerResponses,
			row_number() over (partition by RecommenderLogId, lower(coalesce(cast(json_extract_scalar(a,
				'$.IsAvailableToTryInStore') as string), cast(json_extract_scalar(a,
				'$.isAvailableToTryInStore') as string))) ) as array_idx
		from
			json_array_table 
	)

	select distinct
		cast(json_extract(a,
				'$.VariantId') as string) as VariantId,
		RecommenderLogId,
		array_idx as RecommendationOrder,
		coalesce(coalesce(coalesce(json_extract_scalar(a,
			'$.Sku'), json_extract_scalar(a,
			'$.SKU') ), json_extract_scalar(a,
			'$.sku')), json_extract_scalar(a,
			'$.AX2012ProductID')) as RecommendationItem,
		LastModifiedOn as LastModifiedOn,
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
create temp table RecommendationsDataVariantTemp as
select distinct
	RecommenderLogId,
	RecommendationOrder,
	RecommendationItem,
	RecommenderationRank,
	cast(json_extract_scalar(var,
			'$.size') as string) as MattressSize,
	cast(json_extract_scalar(var,
			'$.variantId') as string) as VariantId,
	cast(json_extract_scalar(var,
			'$.olpPrice') as numeric) as OLPPrice,
	cast(json_extract_scalar(var,
			'$.salesPrice') as numeric) as SalesPrice,
	LastModifiedOn,
	D365ProductID,
	AX2012ProductID,
	Sku,
	cast(json_extract_scalar(var,
			'$.d365RecordId') as string) as D365RecordId,
	'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' as CreatedBy,
	'DAG - load-recommenderlog-customerprofile-data-to-bq-v1.0' as ModifiedBy,
	current_datetime('America/Chicago') as CreatedDatetime,
	current_datetime('America/Chicago') as ModifiedDatetime
from (
		with
		json_array_table as (
			select
				RecommenderLogId,
				LastModifiedOn,
				json_text as a,
				CustomerResponses
			from
				`prod-mattressfinder-project.mattressfinder_staging_data.mf_recommender_log_cdc_data_stg` rlstg3
			left join
				unnest(coalesce((json_extract_array(json_extract(ResponsePayload,
					'$.RecommendedProduct'))), (json_extract_array(json_extract(ResponsePayload,
					'$.recommendedProduct'))))) as json_text
			where
				cast(rlstg3.LastModifiedDate as date) > cast('2021-03-30' as date) 
		),

		json_array_element_table as (
			select
				RecommenderLogId,
				LastModifiedOn,
				a,
				CustomerResponses,
				row_number() over (partition by RecommenderLogId, lower(coalesce(cast(json_extract_scalar(a,
					'$.IsAvailableToTryInStore') as string), cast(json_extract_scalar(a,
					'$.isAvailableToTryInStore') as string))) ) as array_idx
			from
				json_array_table 
		)

		select distinct
			cast(json_extract(a,
				'$.VariantId') as string) as VariantId,
			RecommenderLogId,
			array_idx as RecommendationOrder,
			coalesce(coalesce(coalesce(json_extract_scalar(a,
				'$.Sku'), json_extract_scalar(a,
				'$.SKU') ), json_extract_scalar(a,
				'$.sku')), json_extract_scalar(a,
				'$.AX2012ProductID')) as RecommendationItem,
			LastModifiedOn as LastModifiedOn,
			json_extract_scalar(a,
				'$.Rank') as RecommenderationRank,
			cast(json_extract_scalar(a,
				'$.D365ProductID') as string) as D365ProductID,
			json_extract_scalar(a,
				'$.AX2012ProductID') as AX2012ProductID,
			coalesce(coalesce(json_extract_scalar(a,
				'$.Sku'), json_extract_scalar(a,
				'$.SKU') ), json_extract_scalar(a,
				'$.sku')) as Sku
		from
			json_array_element_table)A
left join
	unnest(json_extract_array(VariantId)) var
where
	RecommenderLogId not in (
		select distinct recItems.RecommenderLogId
		from
			`prod-mattressfinder-project.mattressfinder_data.mf_recommendations_data_variant` recItems)
order by
	1,
	2;

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
set scriptjob_id = (select @@script.job_id);
set message = concat('After "2021-03-30T00:00:00.00" - ', scriptjob_id);

insert into `mattress-firm-inc.mfrm_config_logging_data.logging_data`
( FunctionName, ProcessedRows, FailedRows, StartDateTime, EndDateTime, Status, TargetTable, Message, JobType, Source, LogID)
values ('sp_load_recommendation_data_variant', rowCount, 0, cast(startdatetime as string), cast(current_datetime('America/Chicago') as string), 'Success',
	'mattressfinder_data.mf_recommendations_data_variant', message, StatusCheck, 'mattressfinder_staging_data.mf_recommender_log_stg', cast(floor(1000 * rand()) as string) || '-' || cast(current_datetime('America/Chicago') as string));


end IF;

-- updating start and end date in jobs config table
update `mattress-firm-inc.mfrm_config_logging_data.jobs_config_data`
set
	StartDate = startdatetime,
	EndDate = timestamp(cast(current_timestamp() as datetime), 'America/Chicago'),
	LastExecutionTime = (select cast(max(LastModifiedOn) as timestamp) from `prod-mattressfinder-project.mattressfinder_data.mf_recommender_log`),
	Key = (select cast(max(RecommenderLogId) as string) from `prod-mattressfinder-project.mattressfinder_data.mf_recommender_log`)

where ConfigName = 'RecommenderLogMainRecommendations';


end;
