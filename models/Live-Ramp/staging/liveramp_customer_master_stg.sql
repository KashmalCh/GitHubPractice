{{config(
    materialized='table'
)}}

select 
 *
from   
 {{source('mfrm_customer_and_social', 'customer_master')}}