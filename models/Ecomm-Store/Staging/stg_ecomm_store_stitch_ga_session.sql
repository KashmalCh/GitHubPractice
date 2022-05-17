{{config(
    materialized = 'table',
    schema = 'stg'
)}}

with EcommStoreStitch_phase1 as
(
	with CandidateClientID as (
		select distinct ClientId from `8460582.ga_sessions_*`
  		where _TABLE_SUFFIX > (Select replace(cast(date_sub(date(LastExecutionTime) , interval 1 day) as STRING),'-','') from `mfrm_config_logging_data.jobs_config_data` where Mode = 'CDC' and ConfigName ='ecomm_store_stitch')
	)
	select
	coalesce (snap1.WrittenDate,a.WrittenDate) as WrittenDate,
		g.ClientID,
		coalesce (snap1.TotalQty,a.TotalQty) as TotalQty,
		coalesce (snap1.TotalSales,a.TotalSales) as TotalSales,
		coalesce (g.TransactionId,a.OrderNumber) as OrderNumber,
    coalesce (store.StoreId,d.StoreId) as StoreId,
    coalesce (snap1.CustomerId,c.CustomerId) as CustomerId,
    coalesce (snap1.SalesId,c.SalesId) as SalesId,
    coalesce (store.PurchaseChannel,d.PurchaseChannel) as PurchaseChannel,
	coalesce (snap1.StitchingID,case when g.TransactionId is not null then 16 end, c.StitchingID) as StitchingID,
  g.FullVisitorID,
		g.source,
		g.medium,
		g.campaign,
		g.hits_page,
		g.date1,
		g.visitStartTime,
		SPLIT(g.ClientID, r'.')[SAFE_OFFSET(1)] as CLientID10,
		g.TransactionId,
		g.adContent,
		g.browser,
		g.visitTime_sec,
		date(visitTime_sec) as visitTime_date,
		g.visitid ,
		g.eventCategory,
		g.eventAction,
		g.eventLabel,
		g.eventValue ,
		g.TRANSACTIONs as Transactions,
		g.transactionRevenue,
        timeOnSite,
		
		-- Group of CASEs to evaluate Bucket from medium, campaign, source
		case
			when g.medium="affiliate" then "Affiliate"
			when REGEXP_CONTAINS(g.campaign,"^(.*Display.*|.*display.*)$") then "Display"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*gdn.*)$")) then "Display"
			when REGEXP_CONTAINS(g.source,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display"
			when REGEXP_CONTAINS(g.medium,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*Awareness.*)$")) then "Social Media"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*Awareness.*)$")) then "Social Media"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_lf.*|.*promo.*)$")) then "Social Media"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*_lf.*|.*promo.*)$")) then "Social Media"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*awareness.*|.*Awareness.*)$")) then "Social Media"
			when (g.medium="social" OR REGEXP_CONTAINS(g.medium,"^(.*social.*|.*facebook.*|.*instagram.*|.*Social.*|.*Facebook.*|.*Instagram.*)$")) then "Social Media"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*local.*)$")) then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.source,"^(.*yelp.*)$") then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.medium,"^(.*yelp.*)$") then "Local - Drive to Store"
			when g.medium="email" then "Email"
			when REGEXP_CONTAINS(g.source,"^(.*auto_trans.*)$") then "Email"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*PLA.*|.*pla.*|.*shopping.*)$")) then "Paid Search"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*nb_.*|.*NB_.*|.*non-brand.*|.*ssb.*)$")) then "Paid Search"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*LIA.*|.*lia.*|.*lias.*)$")) then "Paid Search"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*Brand_.*|.*Local -.*|.*brand_.*|.*local -.*)$")) then "Paid Search"
			when g.source="google my business" then "GMB - Organic"
			when g.medium="organic" then "Organic Search"
			when g.source="organic" then "Organic Search"
			when g.source="(direct)" then "Direct"
			when g.medium="video" then "OLV & OLA"
			when REGEXP_CONTAINS(g.medium,"^(.*video.*|.*youtube.*)$") then "OLV & OLA"
			when REGEXP_CONTAINS(g.campaign,"^(.*26579484.*|.*26578107.*)$") then "OLV & OLA"
			when REGEXP_CONTAINS(g.source,"^(.*audio.*)$") then "OLV & OLA"
			when REGEXP_CONTAINS(g.medium,"^(.*audio.*)$") then "OLV & OLA"
			when g.campaign="xyz" then "Affiliate"
			when REGEXP_CONTAINS(g.medium,"^(.*cpc.*)$") then "Paid Search"
			when REGEXP_CONTAINS(g.source,"^(.*cpc.*)$") then "Paid Search"
			when g.medium="referral" then "Referral"
			else "Other"
		end as Bucket,
		
		-- Group of CASEs to evaluate Marketng_Channel_v4 from medium, campaign, source
		case
			when REGEXP_CONTAINS(g.medium,"^(.*video.*|.*youtube.*)$") then "Video"
			when g.medium="video" then "Video"
			when REGEXP_CONTAINS(g.campaign,"^(.*26578107.*)$") then "Display"
			when REGEXP_CONTAINS(g.campaign,"^(.*26579484.*)$") then "Video"
			when g.medium="affiliate" then "Affiliate"
			when REGEXP_CONTAINS(g.campaign,"^(.*Display.*|.*display.*)$") then "Display"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*gdn.*)$")) then "Display"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*Awareness.*)$")) then "Social - Awareness"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*Awareness.*)$")) then "Social - Awareness"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_lf.*|.*promo.*)$")) then "Social - Promo"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*_lf.*|.*promo.*)$")) then "Social - Promo"
			when (g.medium="social" or REGEXP_CONTAINS(g.source,"^(.*pinterest.*|.*facebook.*|.*instagram.*|.*social.*|.*facebook.*|.*instagram.*)$")) then "Social - Other"
			when (g.medium="social" or REGEXP_CONTAINS(g.medium,"^(.*pinterest.*|.*facebook.*|.*instagram.*|.*social.*|.*facebook.*|.*instagram.*)$")) then "Social - Other"
			when g.medium="email" then "Email"
			when REGEXP_CONTAINS(g.source,"^(.*auto_trans.*|.*auto_pp.*)$") then "Email"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*PLA.*|.*pla.*|.*shopping.*)$")) then "Paid Search - PLA"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*nb_.*|.*NB_.*|.*non-brand.*|.*ssb.*)$")) then "Paid Search - Non Brand"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*LIA.*|.*lia.*|.*lias.*)$")) then "Paid Search - LIA"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*Brand_.*|.*Local -.*|.*brand_.*|.*local -.*)$")) then "Paid Search - Brand"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*local.*)$")) then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.medium,"^(.*yelp.*)$") then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.source,"^(.*yelp.*)$") then "Local - Drive to Store"
			when g.source="google my business" then "GMB - Organic"
			when REGEXP_CONTAINS(g.medium,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display"
			when REGEXP_CONTAINS(g.source,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display"
			when g.medium="organic" then "Organic"
			when g.source="organic" then "Organic"
			when g.source="(direct)" then "Direct"
			when REGEXP_CONTAINS(g.source,"^(.*audio.*)$") then "Audio"
			when REGEXP_CONTAINS(g.medium,"^(.*audio.*)$") then "Audio"
			when g.campaign="xyz" then "Affiliate"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*awareness.*|.*Awareness.*)$")) then "Social - Awareness"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*mfrm.*|.*ntm.*|.*2021.*)$")) then "Paid Search - Other"
			when g.medium="referral" then "Referral"
			else "Other"
		end AS Marketng_Channel_v4 ,
		
		-- Group of CASEs to evaluate Parent_Group from medium, campaign, source
		case
			when g.medium="affiliate" then "Affiliate"
			when REGEXP_CONTAINS(g.campaign,"^(.*Display.*|.*display.*|.*26578107.*)$") then "Display"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*gdn.*)$")) then "Display"
			when REGEXP_CONTAINS(g.source,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display"
			when REGEXP_CONTAINS(g.medium,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*Awareness.*)$")) then "Social Media"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*Awareness.*)$")) then "Social Media"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_lf.*|.*promo.*)$")) then "Social Media"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*_lf.*|.*promo.*)$")) then "Social Media"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*awareness.*|.*Awareness.*)$")) then "Social Media"
			when (g.medium="social" OR REGEXP_CONTAINS(g.medium,"^(.*social.*|.*facebook.*|.*instagram.*|.*Social.*|.*Facebook.*|.*Instagram.*)$")) then "Social Media"
			when g.medium="email" then "Email"
			when REGEXP_CONTAINS(g.source,"^(.*auto_trans.*)$") then "Email"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*PLA.*|.*pla.*|.*shopping.*)$")) then "Paid Search"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*nb_.*|.*NB_.*|.*non-brand.*|.*ssb.*)$")) then "Paid Search"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*LIA.*|.*lia.*|.*lias.*)$")) then "Paid Search"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*Brand_.*|.*Local -.*|.*brand_.*|.*local -.*)$")) then "Paid Search"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*local.*)$")) then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.source,"^(.*yelp.*)$") then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.medium,"^(.*yelp.*)$") then "Local - Drive to Store"
			when g.source="google my business" then "GMB - Organic"
			when g.medium="organic" then "Organic Search"
			when g.source="organic" then "Organic Search"
			when g.source="(direct)" then "Direct"
			when g.medium="video" then "OLV & OLA"
			when REGEXP_CONTAINS(g.medium,"^(.*video.*|.*youtube.*)$") then "OLV & OLA"
			when REGEXP_CONTAINS(g.campaign,"^(.*26579484.*)$") then "OLV & OLA"
			when REGEXP_CONTAINS(g.source,"^(.*audio.*)$") then "OLV & OLA"
			when REGEXP_CONTAINS(g.medium,"^(.*audio.*)$") then "OLV & OLA"
			when g.campaign="xyz" then "Affiliate"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*mfrm.*|.*ntm.*|.*2021.*)$")) then "Paid Search - Other"
			when g.medium="referral" then "Referral"
			else "Other"
		end as Parent_Group,
		
		-- Group of CASEs to evaluate Child_Group from medium, campaign, source		
		CASE
			when g.medium="affiliate" then "Affiliate"
			when g.source="google my business" then "GMB - Organic"
			when REGEXP_CONTAINS(g.campaign,"^(.*display_dynamic.*)$") then "Display - Dynamic"
			when REGEXP_CONTAINS(g.campaign,"^(.*dynamicremarketing.*)$") then "Display - Dynamic"
			when REGEXP_CONTAINS(g.campaign,"^(.*remarketing.*)$") then "Display - Non-Dynamic"
			when REGEXP_CONTAINS(g.campaign,"^(.*Display.*|.*display.*)$") then "Display - Non-Dynamic"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*gdn.*)$")) then "Display - Non-Dynamic"
			when REGEXP_CONTAINS(g.source,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display - Non-Dynamic"
			when REGEXP_CONTAINS(g.medium,"^(.*dfa.*|.*gdn.*|.*display.*|.*ttd.*|.*DFA.*|.*Display.*|.*TTD.*)$") then "Display - Non-Dynamic"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*awareness.*|.*junksleep.*|.*Awareness.*)$")) then "Social - Non-Dynamic"
			when g.campaign="26579484" then "Video"
			when g.campaign="26578107" then "Display - Non-Dynamic"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*_up.*|.*_uf.*|.*junksleep.*|.*awareness.*|.*Awareness.*)$")) then "Social - Organic"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*dpa.*)$")) then "Social - Dynamic"
			when (g.medium="social" and REGEXP_CONTAINS(g.campaign,"^(.*dpa.*)$")) then "Social - Dynamic"
			when (g.medium="paidsocial" and REGEXP_CONTAINS(g.campaign,"^(.*_lf.*|.*promo.*)$")) then "Social - Non-Dynamic"
			when (g.medium="social" and REGEXP_CONTAINS(CONCAT(g.source ,'/', g.medium),"^(.*social.*|.*facebook.*|.*instagram.*|.*Social.*|.*Facebook.*|.*Instagram.*|.*pinterest.*)$")) then "Social - Organic"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*local.*)$")) then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.medium,"^(.*yelp.*)$") then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.source,"^(.*yelp.*)$") then "Local - Drive to Store"
			when REGEXP_CONTAINS(g.source,"^(.*auto_trans.*)$") then "Email - Triggered"
			when (g.medium="email" and REGEXP_CONTAINS(g.source,"^(.*auto.*)$")) then "Email - Triggered"
			when (g.medium="email" and REGEXP_CONTAINS(g.source,"^(.*promo.*)$")) then "Email - Promo"
			when g.medium="email" then "Email - Promo"
			--WHEN (g.medium="organic" and REGEXP_CONTAINS(pageType (Landing Content Group),"^(.*home*|.*city.*|.*region.*|.*indy.*|.*domain.*)$")) THEN "Organic - Brand"
			--WHEN (g.source="organic" and REGEXP_CONTAINS(pageType (Landing Content Group),"^(.*home*|.*city.*|.*region.*|.*indy.*|.*domain.*)$")) THEN "Organic - Brand"
			when g.medium="organic" then "Organic - Non-Brand"
			when g.source="organic" then "Organic - Non-Brand"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*PLA.*|.*pla.*|.*shopping.*)$")) then "PS - PLA"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*nb_.*|.*NB_.*|.*non-brand.*|.*ssb.*)$")) then "PS - Non Brand"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*LIA.*|.*lia.*|.*lias.*)$")) then "PS - LIA"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign, "^(.*Brand_.*|.*Local -.*|.*brand_.*|.*local -.*)$")) then "PS - Brand"
			when g.source="google my business" then "GMB - Organic"
			when g.medium="referral" then "Referral"
			when g.medium="organic" then "Organic"
			when g.source="organic" then "Organic"
			when g.source="(direct)" then "Direct"
			when g.medium="video" then "Video"
			when REGEXP_CONTAINS(concat(g.source ,'/', g.medium),"^(.*audio.*)$") then "Audio"
			when REGEXP_CONTAINS(g.medium,"^(.*video.*|.*youtube.*)$") then "Video"
			when g.campaign="xyz" then "Affiliate"
			when (g.medium="cpc" and REGEXP_CONTAINS(g.campaign,"^(.*mfrm.*|.*ntm.*|.*2021.*)$")) then "PS - Other"
			else "Other"
		end as Child_Group
	from	

	 /* This is a primary dataset source of this ETL Load. This block gets ga_sessions for specific columns of ga_session.
	 For fullLoad of ETL it take all the data of ga_session from history till date. In Case of CDC Load, 
	 it will take ga_sessions that are in delta and take all history of ClientIDs that are in delta of ga_sessions.
	 This dataset has no KeyColumn. All columns are needed to create a uniqueness of record.
	 */
	 (	
		 select  
			--MAX(ClientID) AS ClientID,
			hits.page.pagePath as hits_page,
			date as date1,
			visitStartTime,
      FullVisitorID,
      cast(datetime(TIMESTAMP_SECONDS(visitStartTime),"America/Chicago") as timestamp) as visitTime_sec,
			--SPLIT(ClientID, r'.')[SAFE_OFFSET(1)] AS CLientID10,
			ClientID,
			cast(visitId as string) as visitid,
			trafficSource.adContent adContent,
			trafficSource.source source,
			 trafficSource.medium medium,
			trafficSource.campaign campaign,
			device.browser browser,
			 eventInfo.eventCategory eventCategory,
			 eventInfo.eventAction eventAction,
			 eventInfo.eventLabel eventLabel,
			 eventInfo.eventValue eventValue,
			 totals.TRANSACTIONs TRANSACTIONs, 
			 totals.transactionRevenue transactionRevenue,
			hits.TRANSACTION.TransactionId,
            sum(totals.timeOnSite) as timeOnSite 
		  from
			`8460582.ga_sessions_*`, 
			unnest(hits) as hits 
          --WHERE
   			--	_TABLE_SUFFIX BETWEEN '20220223' AND '20220305'
			where
			_TABLE_SUFFIX >= (Select replace(cast(date_sub(date(LastExecutionTime) , interval 1 day) as STRING),'-','') from `mfrm_config_logging_data.jobs_config_data` where Mode = 'CDC' and ConfigName ='ecomm_store_stitch')
			or
				 ClientId in (
				select ClientId from CandidateClientID
				)
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17 ,18,19
     ) g
	--12,483,020
  --12,478,597
 -- Additional Stitching -> Path 1	
	left join
	/* This dataset SalesId,WrittenDate,StoreId,CustomerId, TotalQty, TotalSales for each OrderNumber. 
	Grain is OrderNumber except for 30 exceptions. OrderNumber or SalesId in this tecomm_store_stitch_ga_session_stgable will be used for joining transactionId of ga_session.
	It is joined with Customer Master to get Master CustCd. If there is no master Custcd, e.g like 200 CustomerId will be used from snapshot table.

	If transaction is found in Snapshot, It will be considered as StitchingID: 17
	*/
	  (
	  select SalesId,WrittenDate,StoreId,TotalQty, TotalSales,StitchingID, coalesce(customer.MasterCustCD, sales.CustomerId) as CustomerId from (
	  select distinct  SalesId,WrittenDate,StoreId,CustomerId, sum(Qty) as TotalQty, sum(Sales) as TotalSales, 17 as StitchingID
	  from  {{ ref('src_fact_sales_written_snapshot') }}
      group by SalesId,WrittenDate,StoreId,CustomerId) sales
	left join
		{{ref('src_customer_master')}} customer
		on concat('AX-',sales.CustomerId) = customer.CustCd
	  ) snap1
	on
	  g.TransactionId=snap1.SalesId 
	left join
	-- Stitching on Store Master to get PurchaseChannel
	  {{ref('src_store_master')}} store
	on
	  store.storeId = snap1.StoreId
    
--Default Route 
	left join
	(
	/*
	This dataset is obtained from rpt_ga_clientid10_store_purchase_sessions. Grain for this dataset is ClientID,GADate. 
	For each listed web impression, we get daily highest order order number against a clientId. Some other attributes of Order are also obtained.
	Adding row number function to deduplicate data on clientid 10 and ga date and picking up order that have highest sales value
	It will be joined with ga_session on the basis of clientId and ga_data, so that we can associate each web impression with highest Order value placed on a day.
	*/ 
	
	select * from (
		select
			GADateFormatted,
			GADate,
			OrderNumber,
			case when ExtractedClientID = 'undefined' then ClientID else ExtractedClientID end as ClientID,
			row_number() OVER (partition by OrderNumber order by DateHourMinute asc) RNo
			--ROW_NUMBER() OVER (PARTITION BY OrderNumber,ClientID,GADate ORDER BY DateHourMinute ASC) RNo
		from (
			select
			GADate,
			GADateFormatted,
			DateHourMinute,
			replace(replace(substr(EventLabel, 1, instr(EventLabel, "|", 1, 1)),"|",""),"AX-","") OrderNumber,
			replace(substr(EventLabel, instr(EventLabel, "|", 1, 1) + 1, instr(EventLabel, "|", 1, 2) - instr(EventLabel, "|", 1, 1)),"|","") ExtractedClientID,
			ClientID,
			from {{source('mfrm_reporting_dataset_dynamic','rpt_ga_post_purchase_sessions')}} ) 
			)rpps
		left join
		(
		select
			SalesId, max(WrittenDate) WrittenDate, sum(qty) TotalQty, sum(sales) TotalSales
		from    {{source('mfrm_sales_dynamic','fact_sales_written_snapshot')}}
		group by SalesId
			--1,611 orders have more than one written date, therfore picking up the latest one
			) fsws
		on
		rpps.OrderNumber = fsws.SalesId
		where
		RNo = 1
		and rpps.GADateFormatted between fsws.WrittenDate
		and fsws.WrittenDate+30
	
	)a	
	on
	  a.ClientID= g.ClientID  and  g.date1 = a.GADate 
      
  left join
	  (
	  	/*
	This dataset is obtained from fact_sales_written_snapshot. Grain for this dataset is SalesId except for 30 exceptions. 
	It is joined with Customer Master to get Master CustCd. If there is no master Custcd, e.g like 200 CustomerId will be used from snapshot table.
	If transaction is found in Snapshot, It will be considered as StitchingID: 18.
	It will be joined with Order number retrieved from previous join, so that order details can be extended from this table.
	*/
	  select distinct
	  SalesId,WrittenDate,StoreId, coalesce(customer.MasterCustCD, sales.CustomerId) as CustomerId, 18 as StitchingID
	  from 
	  {{source('mfrm_sales_dynamic','fact_sales_written_snapshot')}} sales
	  left join
		{{source('mfrm_customer_and_social_dynamic','customer_master')}} customer
		on concat('AX-',sales.CustomerId) = customer.CustCd
	  
	  ) c
	on
	  a.OrderNumber=c.SalesId and a.WrittenDate=c.WrittenDate
	left join
	  {{source('mfrm_sales_dynamic','store_master')}} d
	on
	  d.storeId = c.StoreId  
    
) ,
/*This CTE will create a Master record of a web impression. 
It will self join ga_session table in such a way that for each clientId and Date OrderDetails are stitched.
If there are no Order details associated with ClientId and date. Its record will be excluded. This master impression of ClientId, date and Order Details will be used in filling up Web impressions that have no order details but have same client Id or CustomerId */
Historical_Visit_Stiching as (
			select *, row_number() OVER(partition by ClientID,date1 order by WrittenDate, TotalSales desc) as RowNumber
			from (
					select distinct p1.ClientID,p1.date1, p2.WrittenDate,p2.TotalQty,p2.TotalSales,p2.OrderNumber,p2.StoreId,p2.CustomerId,p2.PurchaseChannel,p2.SalesId, p2.StitchingID
					from EcommStoreStitch_phase1 p1
					inner join EcommStoreStitch_phase1 p2 on p1.ClientID = p2.ClientID 
					and p2.date1 >= p1.date1 
					and p2.WrittenDate is not null
			)
)

select  case when p1.WrittenDate is null and hvs.WrittenDate is not null then hvs.WrittenDate  else p1.WrittenDate end as WrittenDate,
        p1.ClientID,
        case when p1.TotalQty is null and hvs.TotalQty is not null then hvs.TotalQty  else p1.TotalQty end as TotalQty,
        case when p1.TotalSales is null and hvs.TotalSales is not null then hvs.TotalSales  else p1.TotalSales end as TotalSales,
        case when p1.OrderNumber is null and hvs.OrderNumber IS not null then hvs.OrderNumber  else p1.OrderNumber end as OrderNumber,
        case when p1.StoreId is null and hvs.StoreId is not null then hvs.StoreId  else p1.StoreId end as StoreId,
        case when p1.CustomerId is null and hvs.CustomerId is not null then hvs.CustomerId  else p1.CustomerId end as CustomerId,--SalesId
		case when p1.SalesId is null and hvs.SalesId is not null then hvs.SalesId  else p1.SalesId end as SalesId,--SalesId
        case when p1.PurchaseChannel is null and hvs.PurchaseChannel is not null then hvs.PurchaseChannel  else p1.PurchaseChannel end as PurchaseChannel,
		case when p1.StitchingID is null and hvs.StitchingID is not null then hvs.StitchingID  else p1.StitchingID end as StitchingID,
		p1.FullVisitorID,
    p1.source,
		p1.medium,
		p1.campaign,
		p1.hits_page,
		p1.CLientID10,
		p1.TransactionId,
		p1.adContent,
		p1.browser,
		p1.visitTime_sec,
		date(p1.visitTime_sec) as visitTime_date,
		p1.visitid ,
		p1.eventCategory,
		p1.eventAction,
		p1.eventLabel,
		p1.eventValue ,
		p1.Transactions,
		p1.transactionRevenue,
		Bucket,
		Marketng_Channel_v4,
		Parent_Group,
		Child_Group,
		case  when --date(visitTime_sec) <= (CASE WHEN p1.WrittenDate IS NULL AND hvs.WrittenDate IS NOT NULL THEN hvs.WrittenDate  ELSE p1.WrittenDate END) 
		-- Check to see if visitTime and WrittenDate difference is not older than 30 days
		--and 
		DATE_DIFF((case when p1.WrittenDate is null and hvs.WrittenDate is not null then hvs.WrittenDate else p1.WrittenDate end),date(visitTime_sec),  day) <= 30 and DATE_DIFF((case when p1.WrittenDate is null and hvs.WrittenDate is not null then hvs.WrittenDate  else p1.WrittenDate end),date(visitTime_sec),  day) >= 0
			then "YES"
			else "NO"
		end as web_first,
        timeOnSite,
        
from EcommStoreStitch_phase1 p1
left join Historical_Visit_Stiching hvs on p1.ClientID = hvs.ClientID and p1.date1 = hvs.date1 and hvs.RowNumber=1