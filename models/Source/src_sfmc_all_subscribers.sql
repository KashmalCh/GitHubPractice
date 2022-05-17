{{config(
    materialized = 'view',
	schema = 'src'
)}}

select * from  {{source('mfrm_customer_and_social', 'sfmc_all_subscribers')}}