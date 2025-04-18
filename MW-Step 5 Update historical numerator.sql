-- 1st step (check the latest month)
/*
select WM_YEAR_NBR, WM_MONTH_NBR, sum(wm_sales)
from wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb
group by 1, 2
order by 1, 2
*/

--2nd FY26Feb and historical data
create or replace table wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb_Total --update
as 
select *
from wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY25Jan_Total --update with last month
where (WM_YEAR_NBR, WM_MONTH_NBR) != (2024,10)
and (WM_YEAR_NBR, WM_MONTH_NBR) != (2024,11)
and (WM_YEAR_NBR, WM_MONTH_NBR) != (2024, 12)
and (WM_YEAR_NBR, WM_MONTH_NBR) != (2025, 1)-- easy to replace
union distinct
select *
from wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb  --update
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;


/*
--3rd
select WM_YEAR_NBR, WM_MONTH_NBR, sum(wm_sales)
from wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb_Total
group by 1, 2
order by 1, 2
*/
