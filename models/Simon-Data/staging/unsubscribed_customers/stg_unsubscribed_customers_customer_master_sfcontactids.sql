{{config(
    materialized = 'table',
    schema = 'stg'
)}}
SELECT 
    * 
FROM (
        SELECT DISTINCT
            SFContactID,
            `mattress-firm-inc.mfrm_customer_360.fn_normalize_email`(EmailAddr) AS EmailAddr,
            CustomerId,
            CONCAT(TRIM(UPPER(FName)),' ',TRIM(UPPER(LName))) AS Name,
            ROW_NUMBER() OVER (PARTITION BY SFContactID ORDER BY COALESCE(UpdatedDate,CreatedDate) DESC) as CustomerId_Num
        FROM (    
                SELECT
                    UpdatedDate,
                    CreatedDate,
                    SFContactID,
                    EmailAddr,
                    FName,
                    LName,
                    COALESCE(MasterCustCd,CustCD) as CustomerID
                FROM 
                     {{ ref("src_customer_master")}}
        ) AS AA
    )  
WHERE CustomerId_Num=1