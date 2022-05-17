{{config(
    materialized='table'
)}}

select  
 distinct MasterCustCD 
from 
 {{source('marketing', 'demo_Jan22_customers_02012022')}}