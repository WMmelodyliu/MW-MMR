with cte as (

SELECT wm_year_nbr, wm_week_nbr, UPC_NBR,ITEM1_DESC,ITEM2_DESC, SIGNING_DESC, UPC_DESC, CONSUMER_ITEM_DESC, PRODUCT_DESC, ACCTG_DEPT_NBR
FROM
(
  SELECT mds_fam_id, UPC_NBR,ITEM1_DESC,ITEM2_DESC, SIGNING_DESC, UPC_DESC, CONSUMER_ITEM_DESC, PRODUCT_DESC, ACCTG_DEPT_NBR
  FROM `wmt-edw-prod.WW_CORE_DIM_VM.ITEM_DIM` id
  WHERE id.ACCTG_DEPT_NBR IN (82) 
  OR (id.dept_catg_grp_desc = "DOTCOM" OR 
  id.dept_catg_grp_desc = "DOT COM" OR
  id.dept_category_desc = "DOTCOM" OR 
  id.dept_category_desc = "DOT COM" OR
  id.dept_subcatg_desc = "DOTCOM" OR 
  id.dept_subcatg_desc = "DOT COM")
  AND id.current_ind = 'Y'
  AND id.base_div_nbr = 1
  AND id.country_code='US'
  GROUP BY mds_fam_id, UPC_NBR,ITEM1_DESC,ITEM2_DESC, SIGNING_DESC, UPC_DESC, CONSUMER_ITEM_DESC, PRODUCT_DESC, ACCTG_DEPT_NBR
) id
INNER JOIN
(
  SELECT store_nbr, scan_id, visit_date
  FROM `wmt-edw-prod.US_WM_MB_VM.SCAN` s
  WHERE s.scan_type =0  
  AND s.OTHER_INCOME_IND IS NULL
  AND s.visit_date >= DATE_SUB(CURRENT_DATE(),INTERVAL 9 DAY)
  and s.visit_date <= DATE_SUB(CURRENT_DATE(),INTERVAL 3 DAY)
  GROUP BY store_nbr, scan_id, visit_date
) s
ON id.mds_fam_id = s.scan_id 
INNER JOIN
(
  SELECT store_nbr
  FROM `wmt-edw-prod.WW_CORE_DIM_VM.STORE_DIM` sd
  WHERE sd.country_code = 'US'
  AND sd.base_div_nbr = 1 --Walmart only
  AND sd.current_ind = 'Y'
  AND sd.state_prov_code NOT IN('PR')
  GROUP BY store_nbr
) sd
ON s.store_nbr = sd.store_nbr
INNER JOIN 
(
  SELECT calendar_date, wm_year_nbr, wm_week_nbr
  FROM `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM`
) AS c -- Calendar table: Select "WM" Calendar year not fiscal
ON s.visit_date = c.calendar_date
GROUP BY wm_year_nbr, wm_week_nbr, UPC_NBR,ITEM1_DESC,ITEM2_DESC, SIGNING_DESC, UPC_DESC, CONSUMER_ITEM_DESC, PRODUCT_DESC, ACCTG_DEPT_NBR)
select wm_year_nbr,
 wm_week_nbr, 
 UPC_NBR,
REGEXP_Replace(item1_desc, "[^\x20-\x7e]+", "") as ITEM1_DESC,
REGEXP_Replace(item2_desc, "[^\x20-\x7e]+", "") as ITEM2_DESC,
REGEXP_Replace(signing_desc, "[^\x20-\x7e]+", "") as signing_desc,
REGEXP_Replace(upc_desc, "[^\x20-\x7e]+", "") as upc_desc,
REGEXP_Replace(consumer_item_desc, "[^\x20-\x7e]+", "") as consumer_item_desc,
REGEXP_Replace(product_desc, "[^\x20-\x7e]+", "") as product_desc,
ACCTG_DEPT_NBR,
from (select wm_year_nbr,
wm_week_nbr,
UPC_NBR,
replace(Replace(Replace(ITEM1_DESC, ",", ""), "'", ""), '"', "") AS ITEM1_DESC,
replace(Replace(Replace(ITEM2_DESC, ",", ""), "'", ""), '"', "") AS ITEM2_DESC,
replace(Replace(Replace(signing_desc, ",", ""), "'", ""), '"', "") AS signing_desc,
replace(Replace(Replace(upc_desc, ",", ""), "'", ""), '"', "") AS upc_desc,
replace(Replace(Replace(consumer_item_desc, ",", ""), "'", ""), '"', "") AS consumer_item_desc,
replace(Replace(Replace(product_desc, ",", ""), "'", ""), '"', "") AS product_desc,
acctg_dept_nbr
from cte
)

## if we need historic data 
-- 9,3; 16,10;23,17
