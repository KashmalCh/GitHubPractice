{{config(
    materialized='table'
)}}

 SELECT DISTINCT COALESCE(MasterCustCD, CustCd) AS MasterCustCD,
COALESCE(MasterCustCD, CustCd) AS customer_code,
FName,
LName,
EmailAddr,
BusPhone,
HomePhone,
Addr1,
Addr2,
County,
City,
StCd,
ZipCd,
CASE WHEN SPLIT(COALESCE(MasterCustCD, CustCd), "-")[SAFE_OFFSET(1)] IN (SELECT * FROM {{ref('liveramp_fact_sales_written_snapshot_stg')}} ) THEN 1 ELSE 0 END AS Customer_with_Transaction_Flag
FROM {{ref('liveramp_customer_master_stg')}}
WHERE COALESCE(MasterCustCD, CustCd) NOT IN (SELECT * FROM {{ref('liveramp_demo_all_customer_10272021_stg')}})
AND COALESCE(MasterCustCD, CustCd) NOT IN (SELECT * FROM {{ref('liveramp_demo_Nov21_customers_12012021_stg')}})
AND COALESCE(MasterCustCD, CustCd) NOT IN (SELECT * FROM {{ref('liveramp_demo_Dec21_customers_01052022_stg')}})
AND COALESCE(MasterCustCD, CustCd) NOT IN (SELECT * FROM {{ref('liveramp_demo_Jan22_customers_02012022_stg')}})