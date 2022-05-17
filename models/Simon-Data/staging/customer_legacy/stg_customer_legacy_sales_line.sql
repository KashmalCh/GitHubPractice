{{config(
    materialized = 'table',
    schema = 'stg'
)}}

select 
distinct ItmCd, 
DelDocNum, 
Sales, 
IsReturn, 
VariantID, 
Class, 
ProductType, 
Segment, 
Vendor, 
Brand, 
ModelName, 
IsMattress, 
IsAdjustable, 
Channel, 
CreatedDate
from
(
    select
    distinct
    ItmCd,
    DelDocNum,
    LineAmount as Sales,
    (case
        when qty < 0 
        then True
        else False
        end) as IsReturn,
    VariantID, 
    Class, 
    ProductType, 
    Segment, 
    Vendor, 
    Brand, 
    ModelName, 
    IsMattress, 
    IsAdjustable, 
    Channel, 
    CreatedDate
    from
    {{ref("src_sales_lines")}} sl
      --  `mattress-firm-inc.mfrm_sales.sales_lines` sl
    join
    (
        select 
        distinct VariantID, 
        Class, 
        ProductType, 
        Segment, 
        Vendor, 
        Brand, 
        ModelName, 
        IsMattress, 
        IsAdjustable, 
        Channel, 
        CreatedDate
        from
        (
            select
            ItemCD as VariantID,
            Class,
            ProductType,
            Segment,
            Vendor,
            Brand,
            ModelName,
            (case 
                when producttype = 'MATTRESS' 
                then true 
                else false 
                end) as IsMattress,
            (case 
                when producttype = 'ADJUSTABLE BASE' 
                then true 
                else false 
                end) as IsAdjustable,
            null as Channel,
            CreateDate as CreatedDate
            --PARSE_DATE("%Y/%m/%d", CreateDate) as CreatedDate
            from 
            {{ref("src_es_item")}}
            --    `dev-mfrm-data.mfrm_sales.es_item`
            union all
            select
            VSN VariantID,
            Class,
            ProductType,
            Segment,
            Vendor,
            Brand,
            ModelName,
            (case 
                when producttype = 'MATTRESS' 
                then true 
                else false 
                end) as IsMattress,
            (case 
                when producttype = 'ADJUSTABLE BASE' 
                then true 
                else false 
                end) as IsAdjustable,
                null as Channel,
                CreateDate  as CreatedDate
            --PARSE_DATE("%Y/%m/%d", CreateDate) as CreatedDate
            from 
            {{ref("src_es_item")}}

               -- `dev-mfrm-data.mfrm_sales.es_item`
            union all
            select
            VariantID,
            Class,
            ProductType,
            Segment,
            Vendor,
            Brand,
            ModelName,
            IsMattress,
            IsAdjustable,
            Channel,
            date(CreatedDate) as CreatedDate
            from 
            {{ref("src_product_master")}}
               -- `mattress-firm-inc.mfrm_sales.product_master`
        )
    )pm
    on sl.ItmCd=pm.VariantID
    union all
    select
    distinct
    ItmCd,
    DelDocNum,
    LineAmount as Sales,
    (case
        when qty < 0 
        then True
        else False
        end) as IsReturn,
    VariantID, 
    Class, 
    ProductType, 
    Segment, 
    Vendor, 
    Brand, 
    ModelName, 
    IsMattress, 
    IsAdjustable, 
    Channel, 
    CreatedDate
    from
    {{ref("src_sales_lines")}} sl
       -- `mattress-firm-inc.mfrm_sales.sales_lines` sl
    join
    (
        select 
        distinct VariantID, 
        Class, 
        ProductType, 
        Segment, 
        Vendor, 
        Brand, 
        ModelName, 
        IsMattress, 
        IsAdjustable, 
        Channel, 
        CreatedDate
        from
        (
            select
            ItemCD VariantID,
            Class,
            ProductType,
            Segment,
            Vendor,
            Brand,
            ModelName,
            (case 
                when producttype = 'MATTRESS' 
                then true 
                else false 
                end) as IsMattress,
            (case 
                when producttype = 'ADJUSTABLE BASE' 
                then true 
                else false 
                end) as IsAdjustable,
            null as Channel,
            CreateDate as CreatedDate
            --PARSE_DATE("%Y/%m/%d", CreateDate) as CreatedDate
            from 
            {{ref("src_es_item")}}
               -- `dev-mfrm-data.mfrm_sales.es_item`
            union all
            select
            VSN VariantID,
            Class,
            ProductType,
            Segment,
            Vendor,
            Brand,
            ModelName,
            (case 
                when producttype = 'MATTRESS' 
                then true 
                else false 
                end) as IsMattress,
            (case 
                when producttype = 'ADJUSTABLE BASE' 
                then true 
                else false 
                end) as IsAdjustable,
            null as Channel,
            CreateDate as CreatedDate
            --PARSE_DATE("%Y/%m/%d", CreateDate) as CreatedDate
            from 
            {{ref("src_es_item")}}
              --  `dev-mfrm-data.mfrm_sales.es_item`
            union all
            select
            VariantID,
            Class,
            ProductType,
            Segment,
            Vendor,
            Brand,
            ModelName,
            IsMattress,
            IsAdjustable,
            Channel,
            date(CreatedDate) as CreatedDate
            from 
            {{ref("src_product_master")}}
              --  `mattress-firm-inc.mfrm_sales.product_master`
        )
    )pm
    on split(sl.itmcd, '-')[safe_offset(1)]=pm.VariantID
)