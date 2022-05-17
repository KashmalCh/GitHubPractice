{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select distinct
calendar.calendardate,
calendar.fyearno,
calendar.fqtrno,
calendar.fperiodno,
calendar.fweekno,
store_master.storeid,
store_master.market,
store_master.signagetype,
store_master.region,
store_master.StoreStatus,
store_master.type,
store_master.PurchaseChannel,
store_master.Division
from
(
    select distinct 
    calendardate,
    fyearno,
    fqtrno,
    fperiodno,
    fweekno
    from 
        {{source('mfrm_sales', 'dim_calendar')}}
    where 
        calendardate >= (select 
                            (cast(timestamp_sub(LastExecutionTime, interval 2 day)  as date)) 
                        from 
                            `mfrm_config_logging_data.jobs_config_data`  
                        where 
                            ConfigName = 'store_day')
    and 
        calendardate <= current_date()
) as calendar
cross join
(
    select distinct
    storeid,
    sqft,
    market,
    signagetype,
    region,
    StoreStatus,
    type,
    PurchaseChannel,
    Division
    from 
        {{source('mfrm_sales', 'store_master')}} as store_master
    where 
        store_master.PurchaseChannel in ('Store', 'Web', 'Chat', 'Phone')
    and 
        store_master.Type in ('Retail', 'Digital')
    and 
        store_master.Division <> 'Corporate'
) as store_master
left join 
    {{ref('store_day_fact_store_written_dates_stg')}} fswd 
on 
    store_master.storeid = fswd.storeid
where  
    case 
        when store_master.StoreStatus = 'C' 
        then calendar.calendarDate between fswd.MinWrittenDate and fswd.MaxWrittenDate
        else calendar.CalendarDate >= fswd.MinWrittenDate
        end
