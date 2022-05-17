
 {{
  config(
    materialized='view',
    schema='stg'

  )
}}

  select
  StoreID,
  PurchaseChannel ,
  date(CreatedDateTime) as CreatedDateTime
  from
  {{ ref("src_store_master") }}