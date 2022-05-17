{{ config(
  materialized='view',
	schema = 'src'
) }}

select * from  {{ source('mfrm_customer_360_prod', 'fact_podium_messages_and_conversations') }}