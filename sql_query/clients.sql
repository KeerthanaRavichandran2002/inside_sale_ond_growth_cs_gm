SELECT 
	DISTINCT
	c.id  AS client_fk ,
	c.unique_id as unique_id  ,
	closed_date ,
	c.onboarded_at AS onboarded_date,
	c.client_name ,
	CASE WHEN toDate(onboarded_date) > toDate(closed_date) THEN toDate(onboarded_date) ELSE toDate(closed_date) END AS "final onboarded date",
CASE WHEN toDate(c.onboarded_at) < toDate('2023-09-01') THEN 're activated' ELSE 'new' END AS "type of client"
from metrics.inside_sales_ond a
FULL OUTER  JOIN `user`.Clients c ON a.unique_id = c.unique_id 
WHERE toDate(closed_date) >= toDate('2023-10-01')
AND a.unique_id IS NOT NULL 
ORDER BY  c.id 