{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select 
distinct StoreID, 
PurchaseChannel, 
ESModifiedDateTime as CreatedDateTime, 
WrittenDate, 
SalesID, 
DelDocNum, 
CustCd
from
(
    select
    distinct sh.CustCD as CustCD, 
    StoreID, 
    PurchaseChannel, 
    sh.ESModifiedDateTime, 
    cast(SoWrDt as date) as WrittenDate, 
    sh.DelDocNum as SalesID,
    sh.DelDocNum 
    from
    {{ref('src_sales_header')}} sh
       -- `mattress-firm-inc.mfrm_sales.sales_header` sh
    join 
    (
        select
        StoreID,
        PurchaseChannel ,
        date(CreatedDateTime) as CreatedDateTime
        from 
        {{ref('src_store_master')}}
         --   `mattress-firm-inc.mfrm_sales.store_master`
    )sm 
    on
        sh.SoStoreCd  = sm.StoreID
    union all
    select
    distinct sh.CustCD as CustCD, 
    StoreID, 
    PurchaseChannel, 
    sh.ESModifiedDateTime, 
    cast(SoWrDt as date) as WrittenDate, 
    SPLIT(sh.DelDocNum,'-')[Safe_OFFSET(1)] as SalesID, 
    sh.DelDocNum 
    from
    {{ref('src_sales_header')}} sh
     --   `mattress-firm-inc.mfrm_sales.sales_header` sh
    join 
    (
        select
        id as StoreID,
        PurchaseChannel ,
        date(sysDateCreated) as CreatedDateTime
        from 
          {{ref('src_es_store')}}
           -- `dev-mfrm-data.mfrm_sales.es_store`
    )sm 
    on 
        sh.SoStoreCd = sm.StoreID
)