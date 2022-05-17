 {{ config(
  materialized='incremental',
   schema='stg'
) }}

 with lead_customer_data_stg as
 (
  select
  *
  from
    {{ ref("stg_lead_customer_data") }}
  {% if is_incremental() %}
    where date(ModifiedDateTime) > (select  date_sub(max(ModifiedDateTime), interval 3 day ) from {{ this }})
  {% endif %}
  
 )
select  
    SRC.Email
    ,SRC.FirstName
    ,SRC.LastName
    ,SRC.Address
    ,SRC.City
    ,SRC.State
    ,SRC.PodiumReviews
    ,SRC.InmommentSurveys
    ,SRC.TealiumVisitor
    ,SRC.RecommenderLog
    ,SRC.BazarVoice
    ,SRC.LiveChat
    ,SRC.SFMCAllSubscriber
    ,SRC.PodiumConversations
    ,SRC.SFMCSentHistory
    ,'sp_load_lead_customer_data'  as CreatedBy
    ,'sp_load_lead_customer_data'  as ModifiedBy
    ,CURRENT_DATETIME("America/Chicago") as CreatedDateTime
    ,CURRENT_DATETIME("America/Chicago") as ModifiedDateTime
from 
  lead_customer_data_stg SRC

 
  