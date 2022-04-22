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
        WrittenDate as WrittenDate,
        ClientID as ClientID,
        TotalQty as TotalQuantity,
        TotalSales as TotalSales,
        OrderNumber as OrderNumber,
        StoreId as StoreId,
        CustomerId as CustomerId,
        SalesId as SalesId,
        PurchaseChannel as PurchaseChannel,
        FullVisitorID as FullVisitorID,
        Source as Source,
        Medium as Medium,
        Campaign as Campaign,
        Hits_Page as HitsPage,
        CLientID10 as CLientID10,
        TransactionId as TransactionId,
        AdContent as AdContent,
        Browser as Browser,
        VisitTime_Sec as VisitTimeSec,
        VisitTime_Date as VisitTimeDate,
        Visitid as VisitID,
        EventCategory as EventCategory,
        EventAction as EventAction,
        EventLabel as EventLabel,
        EventValue as EventValue,
        Transactions as Transactions,
        TransactionRevenue as TransactionRevenue,
        Bucket as Bucket,
        Marketng_Channel_V4 as MarketngChannelV4,
        Parent_Group as ParentGroup,
        Child_Group as ChildGroup,
        Web_First as WebFirst,
        cast(null as string) as CustomerWebFirst,
        TimeOnSite as TimeOnSite,
        StitchingID as StitchingID,
        'C360 sp_load_ecomm_store_stitch' as CreatedBy,
        'C360 sp_load_ecomm_store_stitch' as ModifiedBy,
        farm_fingerprint(
            concat(
                coalesce(cast(FullVisitorID as string), ''),
                '-',
                coalesce(cast(ClientID as string), ''),
                '-',
                coalesce(cast(Hits_Page as string), ''),
                '-',
                coalesce(cast(VisitTime_Sec as string), ''),
                '-',
                coalesce(cast(CLientID10 as string), ''),
                '-',
                coalesce(cast(Visitid as string), ''),
                '-',
                coalesce(cast(AdContent as string), ''),
                '-',
                coalesce(cast(Source as string), ''),
                '-',
                coalesce(cast(Medium as string), ''),
                '-',
                coalesce(cast(Campaign as string), ''),
                '-',
                coalesce(cast(Browser as string), ''),
                '-',
                coalesce(cast(EventCategory as string), ''),
                '-',
                coalesce(cast(EventAction as string), ''),
                '-',
                coalesce(cast(EventLabel as string), ''),
                '-',
                coalesce(cast(EventValue as string), ''),
                '-',
                coalesce(cast(TRANSACTIONs as string), ''),
                '-',
                coalesce(cast(TransactionRevenue as string), ''),
                '-',
                coalesce(cast(TransactionId as string), ''),
                '-',
                coalesce(cast(TimeOnSite as string), ''),
                '-',
                coalesce(cast(StoreId as string), ''),
                '-',
                coalesce(cast(SalesId as string), ''),
                '-',
                coalesce(cast(WrittenDate as string), ''),
                '-',
                coalesce(cast(CustomerId as string), '')
            )
        ) as UniqueID,
        current_datetime("America/Chicago") as CreatedDateTime,
        current_datetime("America/Chicago") as ModifiedDateTime
    from
        {{ref('ecomm_store_stitch_ga_session_stg')}}
)

select distinct * from EcommStoreStitch
