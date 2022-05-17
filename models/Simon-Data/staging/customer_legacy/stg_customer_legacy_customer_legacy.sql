{{config(
    materialized = 'table',
	schema = 'stg'
)}}

select 
distinct a.MasterCustCD,
a.WrittenDate,
a.SalesId,
cast(null as string) as StoreId,
a.IsReturn,
cast(null as string) as PurchasePurpose,
cast(null as string) as VariantID,
cast(null as string) as Class,
cast(null as string) as ProductType,
cast(null as string) as Segment,    
cast(null as string) as Vendor,    
cast(null as string) as Brand,    
cast(null as string) as ModelName,    
cast(null as Boolean) as IsMattress,   
cast(null as Boolean) as IsAdjustable,
cast(null as string) as Channel,
cast(null as string) as PurchaseChannel,
a.Sales_sum as Sales,
cCreatedDateTime as CreatedDateTime,
cUpdatedTime as UpdatedTime
from
(
	select
	distinct sh.CustCD as CustCD,
	cm.MasterCustCD ,
	cast(SoWrDt as date) as WrittenDate,
	sh.DelDocNum SalesID,
	sl.ItmCd ProductId,
	LineAmount AS Sales,
	(case
		when qty < 0 
		then True
		else False
		end) as IsReturn,
	sum(LineAmount) OVER (partition by cm.MasterCustCD , sl.ItmCd , sh.DelDocNum  order by SoWrDt) as Sales_sum,
	row_number() OVER(partition by  cm.MasterCustCD ,sl.ItmCd ,sh.DelDocNum   order by SoWrDt) as unique_id,
	cCreatedDateTime,
	cUpdatedTime
	from
	{{ref('src_sales_header')}} sh
	--	`mattress-firm-inc.mfrm_sales.sales_header` sh
	join
		{{ref('src_sales_lines')}} sl

		-- `mattress-firm-inc.mfrm_sales.sales_lines` sl
	on
		sl.DelDocNum = sh.DelDocNum 
	join
	(
		select 
		CustCd,
		MasterCustCD,
		date(ESModifiedDatetime) as ESModifiedDatetime,
		RecordLoadDateTime  as cCreatedDateTime,
		RecordUpdateDatetime as cUpdatedTime,
		from 
		{{ref("src_customer_master")}}
		--	`mattress-firm-inc.mfrm_customer_and_social.customer_master`
		order by 
			ESModifiedDatetime desc
	) cm 
	on 
		sh.custcd =cm.CustCd
)a
left join
	{{ref('stg_customer_legacy_customer_product_sales')}} cps
	on 
		lower(cps.MasterCustCD) = lower(a.MasterCustCD) 
where 
	unique_id = 1 
and 
	cps.MasterCustCD is null  
and 
	a.MasterCustCD is not null 
and 
    a.ProductId is not null 
and 
    a.SalesId is not null
and 
	a.MasterCustCD
	not in
	(
		select 
        distinct MasterCustCD
		from
		{{ref("customer_product_sales")}}
		    
	)