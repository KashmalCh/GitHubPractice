{{config(
    materialized='table'
)}}

select  
 distinct MasterCustCD 
from 
 {{source('marketing', 'demo_Nov21_customers_12012021')}}

