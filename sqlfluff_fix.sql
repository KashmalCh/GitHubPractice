{{config(
    materialized = 'incremental',
    unique_key = 'UniqueID',
    merge_update_columns = ['WrittenDate','ClientID','TotalQuantity','TotalSales','OrderNumber','StoreId','CustomerId','SalesId','PurchaseChannel','FullVisitorID','Source','Medium','Campaign','HitsPage','CLientID10','TransactionId','AdContent','Browser','VisitTimeSec','VisitTimeDate','VisitID','EventCategory','EventAction','EventLabel','EventValue','Transactions','TransactionRevenue','Bucket','MarketngChannelV4','ParentGroup','ChildGroup','WebFirst','CustomerWebFirst','TimeOnSite','StitchingID'],
    post_hook = 'update
                  `mfrm_config_logging_data.jobs_config_data`
                set
                  LastExecutionTime  = (select timestamp_seconds(MAX(visitStartTime)) from `8460582.ga_sessions_*` where _TABLE_SUFFIX > "20200101"),
                  StartDate = cast(current_datetime("America/Chicago") AS timestamp),
                  EndDate = cast(current_datetime("America/Chicago") AS timestamp)
                where
                  ConfigName ="ecomm_store_stitch";'
)}}  
  --com
with EcommStoreStitch as (
select 
farm_fingerprint(concat(coalesce(cast(FullVisitorID as string),''),'-',coalesce(cast(ClientID as string),''),'-',coalesce(cast(hits_page as string),''),'-',coalesce(cast(visitTime_sec as string),''),'-',coalesce(cast(CLientID10 as string),''),'-',coalesce(cast(visitid as string),''),'-',coalesce(cast(adContent as string),''),'-',coalesce(cast(source as string),''),'-',coalesce(cast(medium as string),''),'-',coalesce(cast(campaign as string),''),'-',coalesce(cast(browser as string),''),'-',coalesce(cast(eventCategory as string),''),'-',coalesce(cast(eventAction as string),''),'-',coalesce(cast(eventLabel as string),''),'-',coalesce(cast(eventValue as string),''),'-',coalesce(cast(TRANSACTIONs as string),''),'-',coalesce(cast(transactionRevenue as string),''),'-',coalesce(cast(TransactionId as string),''),'-',coalesce(cast(timeOnSite as string),''),'-',coalesce(cast(StoreId as string),''),'-',coalesce(cast(salesId as string),''),'-',coalesce(cast(WrittenDate as string),''),'-',coalesce(cast(CustomerId as string),'') )) as UniqueID
,WrittenDate as WrittenDate
,ClientID as ClientID
,TotalQty as TotalQuantity 
,TotalSales as TotalSales 
,OrderNumber as OrderNumber
,StoreId as StoreId
,CustomerId as CustomerId
,SalesId as SalesId
,PurchaseChannel as PurchaseChannel
,FullVisitorID as FullVisitorID
,source as Source
,medium as Medium 
,campaign as Campaign 
,hits_page as HitsPage 
,CLientID10 as CLientID10
,TransactionId as TransactionId
,adContent as AdContent
,browser as Browser
,visitTime_sec as VisitTimeSec
,visitTime_date as VisitTimeDate
,visitid as VisitID
,eventCategory as EventCategory
,eventAction as EventAction
,eventLabel as EventLabel
,eventValue as EventValue
,Transactions as Transactions
,transactionRevenue as TransactionRevenue
,Bucket as Bucket
,Marketng_Channel_v4 as MarketngChannelV4
,Parent_Group as ParentGroup
,Child_Group as ChildGroup
,web_first as WebFirst
,cast(null as STRING) as CustomerWebFirst
,timeOnSite as TimeOnSite
,StitchingID as StitchingID
,'C360 sp_load_ecomm_store_stitch' as CreatedBy
,'C360 sp_load_ecomm_store_stitch'  as ModifiedBy
,Current_datetime("America/Chicago") as CreatedDateTime
,Current_datetime("America/Chicago") as ModifiedDateTime
from 
{{ref('ecomm_store_stitch_ga_session_stg')}})
select distinct * from EcommStoreStitch
