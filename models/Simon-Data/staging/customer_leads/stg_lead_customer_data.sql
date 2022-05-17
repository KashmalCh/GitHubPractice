{{ config(
   materialized='table',
  schema='stg'
) }}

select
    `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(Email) AS Email,
    MAX(FirstName) AS FirstName,
    MAX(LastName) AS LastName,
    MAX(Address) AS Address,
    MAX(City) AS City,
    MAX(State) AS State,
    MAX(PodiumReviews) AS PodiumReviews,
    MAX(InmommentSurveys) AS InmommentSurveys,
    MAX(TealiumVisitor) AS TealiumVisitor,
    MAX(RecommenderLog) AS RecommenderLog,
    MAX(BazarVoice) AS BazarVoice,
    MAX(LiveChat) AS LiveChat,
    MAX(SFMC_All_Subscriber) AS SFMCAllSubscriber,
    MAX(PodiumConversations) AS PodiumConversations,
    MAX(SFMCSentHistory) AS SFMCSentHistory,
  "sp_load_data_into_customers_lead" as CreatedBy,
  "sp_load_data_into_customers_lead" as ModifiedBy,
  CAST(Current_DATETIME("America/Chicago") AS TIMESTAMP) as CreatedDateTime	,
  CAST(Current_DATETIME("America/Chicago") AS TIMESTAMP) as ModifiedDateTime,
from (
    select
      A.Email,
      case
        when B.TableName = 'PodiumReviews' then 1
      else
      0
    end
      PodiumReviews,
      case
        when B.TableName = 'InmommentSurveys' then 1
      else
      0
    end
      InmommentSurveys,
      case
        when B.TableName = 'TealiumVisitor' then 1
      else
      0
    end
      TealiumVisitor,
      case
        when B.TableName = 'RecommenderLog' then 1
      else
      0
    end
      RecommenderLog,
      case
        when B.TableName = 'BazarVoice' then 1
      else
      0
    end
      BazarVoice,
      case
        when B.TableName = 'LiveChat' then 1
      else
      0
    end
      LiveChat,
      case
        when B.TableName = 'SFMC_All_Subscriber' then 1
      else
      0
    end
      SFMC_All_Subscriber,
      case
        when B.TableName = 'PodiumConversations' then 1
      else
      0
    end
      PodiumConversations,
      case
        when B.TableName = 'SFMCSentHistory' then 1
      else
      0
    end
      SFMCSentHistory,
      FirstName,
      LastName,
      Address,
      City,
      State
  from  
    {{ ref("stg_lead_customers") }} A
  left join  
    {{ ref("stg_lead_customer_info") }} B
on
  A.Email = B.Email )
group by
    1



 
 