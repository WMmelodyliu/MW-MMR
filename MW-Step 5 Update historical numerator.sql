-- 1st step (check the latest month)
select WM_YEAR_NBR, WM_MONTH_NBR, sum(wm_sales)
from wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb
group by 1, 2
order by 1, 2

--2nd FY26Feb and historical data
CREATE OR REPLACE TABLE wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb_Total --update
AS 
SELECT *
FROM wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY25Jan_Total --update with last month
WHERE (WM_YEAR_NBR, WM_MONTH_NBR) NOT IN (
    SELECT WM_YEAR_NBR, WM_MONTH_NBR
    FROM (
        SELECT distinct WM_YEAR_NBR, WM_MONTH_NBR
        FROM wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY25Jan_Total
        ORDER BY WM_YEAR_NBR DESC, WM_MONTH_NBR DESC
        LIMIT 3
    )
)

UNION DISTINCT

SELECT *
FROM wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb --update
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;
       

--3rd
select WM_YEAR_NBR, WM_MONTH_NBR, sum(wm_sales)
from wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb_Total
group by 1, 2
order by 1, 2
