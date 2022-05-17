{{ config(
  materialized='table',
  schema='stg'
) }}


select
Email,
FirstName,
LastName,
Address,
City,
State,
TableName,
ModifiedDateTime
from (
select 
  Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime,
  ROW_NUMBER() OVER(PARTITION BY Email, TableName ORDER BY ModifiedDateTime DESC) AS R
from (
select
distinct Email,
FirstName,
LastName,
Address,
City,
State,
TableName,
ModifiedDateTime
from
{{ ref("stg_fact_podium_reviews") }}
UNION ALL
select
  DISTINCT Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
{{ ref("stg_fact_inmoment_surveys") }}

UNION ALL
select
  distinct Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
  {{ ref("stg_fact_tealium_visitor") }}
  
UNION ALL
select
  distinct Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
  {{ ref("stg_fact_recommenderlog") }}
 
UNION ALL
select
  distinct Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
  {{ ref("stg_fact_bazaar_voice_interaction") }}

UNION ALL
select
  distinct Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
 {{ ref("stg_fact_sf_live_chat_transcript") }}

UNION ALL
select
  distinct Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
  {{ ref("stg_sfmc_all_subscribers") }}

UNION ALL
select
  distinct Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
  {{ ref("stg_fact_podium_messages_and_conversations") }}

UNION ALL
select
  distinct Email,
  FirstName,
  LastName,
  Address,
  City,
  State,
  TableName,
  ModifiedDateTime
from 
  {{ ref("stg_fact_sfmc_sent_history") }}
))
WHERE
 R=1
