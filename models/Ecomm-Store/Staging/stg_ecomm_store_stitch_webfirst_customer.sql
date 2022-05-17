{{config(
    materialized = 'table',
    schema = 'stg'
)}}

with EcommStoreStitch_OrderNumber_WebFirst_Priority as
(
select  ecom.*, coalesce (ord.web_first,ecom.web_first) as OrderNumber_WebFirst_Priority from {{ref('stg_ecomm_store_stitch_ga_session')}} ecom
left join
(
select * from (
select OrderNumber,WrittenDate,web_first, row_number() OVER(partition by OrderNumber,WrittenDate order by visitTime_date) as RowNumber from {{ref('stg_ecomm_store_stitch_ga_session')}})
where RowNumber =1 and lower(web_first) = 'yes'
)ord
ON ecom.OrderNumber = ord.OrderNumber and ecom.WrittenDate = ord.WrittenDate
)
   

----------------------------------

,EcommStoreStitch_WebFirst_Priority as
(
select  ecom.*, coalesce (ord.OrderNumber_WebFirst_Priority,ecom.OrderNumber_WebFirst_Priority) as CustomerWebFirst from EcommStoreStitch_OrderNumber_WebFirst_Priority ecom
left join
(
select * from (
select CustomerId,OrderNumber_WebFirst_Priority, row_number() OVER(partition by CustomerId order by visitTime_date) as RowNumber from EcommStoreStitch_OrderNumber_WebFirst_Priority)
where RowNumber =1 and lower(OrderNumber_WebFirst_Priority) = 'yes'
)ord
on ecom.CustomerId = ord.CustomerId
)   

----------------------------------

,EcommStoreStitch_ClientId_WebFirst_Priority as
(
select  ecom.*, coalesce (ord.CustomerWebFirst,ecom.CustomerWebFirst) as ClientIdWebFirst from EcommStoreStitch_WebFirst_Priority ecom
left join
(
select * from (
select CustomerId,CustomerWebFirst, row_number() OVER(partition by ClientID order by visitTime_date) as RowNumber from EcommStoreStitch_WebFirst_Priority)
where RowNumber =1 and lower(CustomerWebFirst) = 'yes'
)ord
on ecom.CustomerId = ord.CustomerId
)   
select * from EcommStoreStitch_ClientId_WebFirst_Priority