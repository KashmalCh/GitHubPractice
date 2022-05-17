{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select 
writtendate as calendardate,
storeid,
count(distinct salesid) as tickets,
count(distinct employeeid1) as employees,
sum(cost) as cost,
sum(sales) as sales,
sum(qty) as qty,
sum(discount) as discount
from 
    {{ref('store_day_written_linear_sales_stg')}} as written_sales
group by 
writtendate,
storeid
