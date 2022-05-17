{{config(
    materialized = 'incremental',
    unique_key = "Id",
    post_hook = 'update 
                    `dev-mfrm-data.mfrm_config_logging_data.jobs_config_data`
                set 
                    LastExecutionTime = CURRENT_TIMESTAMP() 
                where 
                    configName = "store_day"'
)}}

select 
(storeid||'-'||calendardate) as Id,  -- table will be merged on this column
storeid as StoreId,
fyearno as FYearNo,
fperiodno as FPeriodNo,
fweekno as FWeekNo,
calendardate as CalendarDate,
region as Region,
StoreStatus AS StoreStatus,
type as Type,
cost as Cost,
sales as Sales,
qty as Qty,
PurchaseChannel as PurchaseChannel,
Division as Division,
(case
    when lower(Division) = 'ecommerce' or lower(Division) = 'ecomm'
    then 'Ecommerce'
    else 'B&M'
    end) as DivisionGroup,  -- This binary value column distnguished between B&M and Ecommerce
discount as Discount,
tickets as Tickets
from 
    {{ref('store_day_sto_day_stg')}}
