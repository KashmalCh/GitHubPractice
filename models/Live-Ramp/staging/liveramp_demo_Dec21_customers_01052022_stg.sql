{{config(
    materialized='table'
)}}

select  
 distinct MasterCustCD 
from 
 {{source('marketing', 'demo_Dec21_customers_01052022')}}