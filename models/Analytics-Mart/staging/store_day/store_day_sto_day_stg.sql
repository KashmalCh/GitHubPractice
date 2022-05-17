{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select 
source.region,
source.StoreStatus,
source.type,
source.PurchaseChannel,
source.Division,
source.storeid,
fyearno,
fperiodno,
fweekno,
source.calendardate,
sum(COALESCE(cost,0)) as cost,
sum(COALESCE(sales,0)) as sales,
sum(COALESCE(QTY,0)) as qty,
sum(COALESCE(discount,0)) as discount,
sum(COALESCE(tickets,0)) as tickets
from 
    {{ref('store_day_all_dates_store_stg')}} as source
left join 
    {{ref('store_day_written_sales_stg')}} as sto_day
    on 
        source.storeid = sto_day.storeid
    and 
        source.calendardate = sto_day.calendardate
group by
source.region,
source.StoreStatus,
source.type,
source.PurchaseChannel,
source.Division,
source.storeid,
fyearno,
fperiodno,
fweekno,
calendardate