{{config(
    materialized = 'incremental'
    ,unique_key = 'UniqueKey'
)}}

SELECT 
    CONCAT(CAST(A.SnapshotWrittenWeek AS STRING),A.ProductId) AS UniqueKey
    ,A.SnapshotWrittenWeek
    ,A.ProductId
    ,A.FYear
    ,A.FWeek
    ,A.FPeriod
    ,A.FQtr
    ,A.SnWrSales
    ,A.SnWrUnits
    ,A.EcomStoreSnWrUnits
    ,A.EcomSnWrUnits
    ,A.SnWrUnitPrice
    ,A.SnWrPosCost
    ,A.SnWrCoreCost
    ,A.SnWrOlp
    ,A.SnWrReturnSales
    ,A.SnWrVi
    ,A.SnWrSalesTicketCount
    ,B.SnapshotWrittenWeek LySnapshotWrittenWeek
    ,B.ProductId LyProductId
    ,B.FYear LyFYear
    ,B.FWeek LyFWeek
    ,B.FPeriod LyFPeriod
    ,B.FQtr LyFQtr
    ,B.SnWrSales LySnWrSales
    ,B.SnWrUnits LySnWrUnits
    ,A.EcomStoreSnWrUnits LyEcomStoreSnWrUnits
    ,A.EcomSnWrUnits LyEcomSnWrUnits
    ,B.SnWrUnitPrice LySnWrUnitPrice
    ,B.SnWrPosCost LySnWrPosCost
    ,B.SnWrCoreCost LySnWrCoreCost
    ,B.SnWrOlp LySnWrOlp
    ,B.SnWrReturnSales LySnWrReturnSales
    ,B.SnWrVi LySnWrVi
    ,B.SnWrSalesTicketCount LySnWrSalesTicketCount
    ,'dbt_pricing_grid_sales_update' AS CreatedBy
    ,'dbt_pricing_grid_sales_update' AS ModifiedBy
    ,CURRENT_DATETIME("America/Chicago") AS CreatedDateTime
    ,CURRENT_DATETIME("America/Chicago") AS ModifiedDateTime
FROM
    (
        SELECT 
            SnapshotWrittenWeek
            ,ProductId
            ,FYear
            ,FWeek
            ,FPeriod
            ,FQtr
            ,SnWrSales
            ,SnWrUnits
            ,EcomStoreSnWrUnits
            ,EcomSnWrUnits
            ,SnWrUnitPrice
            ,SnWrPosCost
            ,SnWrCoreCost
            ,SnWrOlp
            ,SnWrReturnSales
            ,SnWrVi
            ,SnWrSalesTicketCount
        FROM {{ref('pricing_grid_snwrsales')}}
        WHERE FYear = (SELECT FYearNo FROM {{source('mfrm_sales_dynamic', 'dim_calendar')}} WHERE CalendarDate = CURRENT_DATE())
    ) A
LEFT JOIN 
    (
        SELECT 
            SnapshotWrittenWeek
            ,ProductId
            ,FYear
            ,FWeek
            ,FPeriod
            ,FQtr
            ,SnWrSales
            ,SnWrUnits
            ,EcomStoreSnWrUnits
            ,EcomSnWrUnits
            ,SnWrUnitPrice
            ,SnWrPosCost
            ,SnWrCoreCost
            ,SnWrOlp
            ,SnWrReturnSales
            ,SnWrVi
            ,SnWrSalesTicketCount
        FROM {{ref('pricing_grid_snwrsales')}}
        WHERE FYear = (SELECT FYearNo - 1 FROM {{source('mfrm_sales_dynamic', 'dim_calendar')}} WHERE CalendarDate = CURRENT_DATE())
    )B
ON 
    A.ProductId = B.ProductId
    AND A.FWeek = B.FWeek

