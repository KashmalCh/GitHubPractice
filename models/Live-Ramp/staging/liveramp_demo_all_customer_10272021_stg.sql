{{config(
    materialized='table'
)}}

select  
 distinct MasterCustCD 
from 
 {{source('marketing', 'demo_all_customers_10272021')}}