{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select 
writtendate,
storeid,
salesid,
itemnumber,
productid,
employeeid1,
ourlowprice,
sales,
qty,
cost,
discount,
unitprice,
( case
    when segment = 'VALUE' 
    then 'PROMOTIONAL'
    when product_master.producttype = 'ADJUSTABLE BASE' 
    then 'ADJUSTABLE_BASE'
    when product_master.producttype = 'ACCESSORIES' 
    then 'ACCESSORIES'
    when product_master.producttype = 'BOX' 
    then 'BOX'
    else segment
    end ) as segment
from 
    {{source('mfrm_sales', 'fact_sales_written_snapshot')}} as written_sales
left join
    {{source('mfrm_sales', 'product_master')}} as product_master
    on 
        written_sales.productid = product_master.variantid
where 
(
    written_sales.writtendate between (select 
                                            (cast(timestamp_sub(LastExecutionTime, interval 2 day) as date)) 
                                        from 
                                            `mfrm_config_logging_data.jobs_config_data`  
                                        where 
                                            ConfigName = 'store_day'
                                        ) and current_date()
)    
and isexclusionitem=false

