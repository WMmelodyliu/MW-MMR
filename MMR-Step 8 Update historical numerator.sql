/*
--the latest 3 months

SELECT distinct wm_year_nbr, wm_month_nbr
FROM wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn2
ORDER BY wm_year_nbr DESC, wm_month_nbr DESC
LIMIT 3
*/


--NLSN
create or replace table wmt-mint-mmr-mw-prod.MMR_numerator.MMR_Numerator_FY26Mar_Total_NLSN --update
as 
select *
from wmt-mint-mmr-mw-prod.MMR_numerator.MMR_Numerator_FY26Feb_Total_NLSN --update with latest 3 month
where (wm_year_nbr, wm_month_nbr) != (2024, 12)
and (wm_year_nbr, wm_month_nbr) != (2025,1)
and (wm_year_nbr, wm_month_nbr) != (2025, 2)-- easy to replace
union distinct
select *
from wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn2   --update
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;

--NPD

create or replace table wmt-mint-mmr-mw-prod.MMR_numerator.MMR_Numerator_FY26Mar_Total_NPD --update
as 
select *
from wmt-mint-mmr-mw-prod.MMR_numerator.MMR_Numerator_FY26Feb_Total_NPD --update with latest 3 month
where (wm_year_nbr, wm_month_nbr) != (2024, 12)
and (wm_year_nbr, wm_month_nbr) != (2025,1)
and (wm_year_nbr, wm_month_nbr) != (2025, 2)-- easy to replace
union distinct
select *
from wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_npd2   --update
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;
