{{ config(
  materialized='table',
  schema='stg'
) }}



select
distinct Email
from (
select
  distinct Email
from
  {{ ref("stg_fact_podium_reviews") }}

UNION ALL
select
distinct Email
from
  {{ ref("stg_fact_inmoment_surveys") }}
  
UNION ALL
select
distinct Email
from
  {{ ref("stg_fact_tealium_visitor") }}
  
UNION ALL
select
distinct Email
FROM
  {{ ref("stg_fact_recommenderlog") }}

UNION ALL
SELECT
distinct Email
from
  {{ ref("stg_fact_bazaar_voice_interaction") }}
   
UNION ALL
select
 distinct Email
FROM
  {{ ref("stg_fact_sf_live_chat_transcript") }}
 
UNION ALL
select
distinct Email
from
  {{ ref("stg_sfmc_all_subscribers") }}
   
UNION ALL
select
distinct Email
from
  {{ ref("stg_fact_podium_messages_and_conversations") }}
 
UNION ALL
select
distinct Email
from
  {{ ref("stg_fact_sfmc_sent_history") }}
    )