{{config(
    materialized = 'table',
	schema = 'stg'
)}}

with source as
(
	select distinct
	to_hex(md5(cast(coalesce(cast(MasterCustCD as 
		string
	), null) || '-' || coalesce(cast(ProductId as 
		string
	), null) || '-' || coalesce(cast(SalesId as 
		string
	), null) as 
		string
	))) as cdp_unique_id,
	MasterCustCD,
	ProductId,
	WrittenDate,
	SalesId,
	cast(StoreId as string) StoreId,
	IsReturn,
	'' as PurchasePurpose,
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
	PurchaseChannel,
	sum(Sales) over (partition by MasterCustCD, ProductId, SalesId  order by WrittenDate) as Sales,
	row_number() over (partition by  MasterCustCD,ProductId,SalesId   order by WrittenDate) as unique_id,
	cCreatedDateTime  , cUpdatedTime 
	from 
        {{ref('stg_customer_legacy_source_cte')}}
)
select 
distinct s.MasterCustCD,
WrittenDate,
SalesId,
StoreId,
IsReturn,
PurchasePurpose,
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
PurchaseChannel,
Sales,
cCreatedDateTime as CreatedDateTime, 
cUpdatedTime UpdatedTime 
from 
	source s
where
	unique_id = 1 
	and 
		s.MasterCustCD is not null 
	and 
		ProductId is not null 
	and 
		SalesId is not null 
	and 
		s.MasterCustCD
		not in
		(
			select 
				distinct MasterCustCD
			from
			    {{ref('customer_product_sales')}}
		
		)