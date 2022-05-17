{{config(
    materialized = 'incremental',
    unique_key = 'MasterCustCD',
    merge_update_columns = ['MasterCustCD', 'PredictedTargetBinaryScores', 'PredictedTargetBinaryClasses', 'ModifiedDateTime']
)}}

select 
MasterCustCD,
predicted_target_binary.scores[OFFSET(1)] as PredictedTargetBinaryScores, 
predicted_target_binary.classes[OFFSET(1)] as PredictedTargetBinaryClasses,
CAST(CURRENT_DATETIME("America/Chicago") AS TIMESTAMP) as ModifiedDateTime,
CAST(CURRENT_DATETIME("America/Chicago") AS TIMESTAMP) as CreatedDateTime
from 
    `dev-mfrm-datascience.clv_propensity_12.predictions_*` 
where 
    _TABLE_SUFFIX = (select 
                            max(_TABLE_SUFFIX) 
                        from 
                            `dev-mfrm-datascience.clv_propensity_12.predictions_*`)
