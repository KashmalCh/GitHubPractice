{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select 
storeid, 
min(WrittenDate) as MinWrittenDate,
max(WrittenDate) as MaxWrittenDate
from 
    {{source('mfrm_sales', 'fact_sales_written_snapshot')}} as written_sales
where 
writtendate > (select 
                    (cast(timestamp_sub(LastExecutionTime, interval 2 day) as date)) 
                from 
                    `mfrm_config_logging_data.jobs_config_data`  
                where 
                    ConfigName = 'store_day') 
                    and writtendate <= current_date()
group by
storeid