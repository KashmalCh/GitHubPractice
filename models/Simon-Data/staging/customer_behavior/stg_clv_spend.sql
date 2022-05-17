{{config(
    materialized = 'incremental',
    unique_key = 'MasterCustCD',
    merge_update_columns = ['MasterCustCD', 'PredictedTargetSales', 'ModifiedDateTime']
)}}

select 
MasterCustCD,
predicted_target_sales.value as PredictedTargetSales,
CAST(CURRENT_DATETIME("America/Chicago") AS TIMESTAMP) as ModifiedDateTime,
CAST(CURRENT_DATETIME("America/Chicago") AS TIMESTAMP) as CreatedDateTime
from 
    `dev-mfrm-datascience.clv_spend_12.predictions_*`
where _TABLE_SUFFIX = (select 
                        max(_TABLE_SUFFIX) 
                        from 
                            `dev-mfrm-datascience.clv_spend_12.predictions_*`)
