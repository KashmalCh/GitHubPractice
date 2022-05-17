{{config(
    materialized = 'table'
)}}

select 
cp.MasterCustCD,
cp.PredictedTargetBinaryClasses as PropensityPredictedTargetBinaryClasses,
cp.PredictedTargetBinaryScores as PropensityPredictedTargetBinaryScores,
cs.PredictedTargetSales as SpendPredictedTargetSales,
from
    {{ref('stg_clv_propensity')}} as cp
left join
    {{ref('stg_clv_spend')}} as cs 
    on 
        cp.MasterCustCD = cs.MasterCustCD
