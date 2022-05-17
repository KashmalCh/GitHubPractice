{{config(
    materialized = 'table'
)}}

SELECT 
  a.Id,
  a.CustomerId,
  a.RatingId,
  a.Author,
  a.AuthorEmailAddress,
  a.AuthorName,
  a.BodyText,
  a.Contact,
  a.Subject,
  a.LastModifiedDate
FROM mattress-firm-inc.mfrm_customer_360.fact_bazaar_voice_interaction a
INNER JOIN
(
 SELECT DISTINCT(CustomerId) FROM 
  mattress-firm-inc.mfrm_sales.fact_sales_written_snapshot 
) b
  ON SPLIT(a.CustomerId, '-')[SAFE_OFFSET(1)] = b.CustomerId
WHERE
a.CustomerId != '-1'
AND a.InteractionTypeId = 'Review'