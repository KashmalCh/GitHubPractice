{{ config(
  materialized='table',
  post_hook='''  DELETE
  FROM
    `mattress-firm-inc.simon_data.customer_leads_cdc`
  WHERE
    `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(Email) IN (
    SELECT
      `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(a.Email)
    FROM
      `mattress-firm-inc.simon_data.customer_leads_cdc` a
    JOIN
      `mattress-firm-inc.mfrm_customer_and_social.customer_master` b
    ON
      `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(a.Email) = `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(b.EmailAddr) );
      
     DROP SCHEMA IF EXISTS simon_data_stg CASCADE;    
'''
) }}

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
,SRC.CreatedDateTime
,CURRENT_DATETIME("America/Chicago") as ModifiedDateTime
from
  {{ ref("stg_cust_leads_cdc") }} SRC


  