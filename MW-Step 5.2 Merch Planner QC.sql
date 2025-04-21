select a.sbu, a.department, a.channel, 
mw_fy25, mw_fy26, round((mw_fy26 - mw_fy25)/nullif(mw_fy25, 0), 3) as mw_yoy,
mp_fy25, mp_fy26, round((mp_fy26 - mp_fy25)/nullif(mp_fy25, 0), 3) as mp_yoy,
round((mp_fy25 - mw_fy25)/nullif(mw_fy25, 0), 3) as fy25_diff,
round((mp_fy26 - mw_fy26)/nullif(mw_fy26, 0), 3) as fy26_diff,
((mw_fy26 - mw_fy25)/nullif(mw_fy25, 0) - (mp_fy26 - mp_fy25)/nullif(mp_fy25, 0)) as MWVsMP_yoy_diff
from (select sbu, department,
channel,
sum(case when WM_YEAR_NBR = 2025 then wm_sales else 0 end) as mw_fy26,
sum(case when WM_YEAR_NBR = 2024 then wm_sales else 0 end) as mw_fy25
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Feb_Total` --update
where  WM_MONTH_NBR = 1 --update
group by 1, 2, 3) as a
inner join (select sbu, department, channel,
sum(case when wm_full_yr_nbr = 2025 then wm_sales else 0 end) as mp_fy26,
sum(case when wm_full_yr_nbr = 2024 then wm_sales else 0 end) as mp_fy25
from (select *
from `wmt-mint-mmr-mw-prod.mw_self_serve_qc.Merch_Planner_FY26Feb` --update
)as a
inner join `wmt-mint-mmr-mw-prod.new_mw_numerator_dev.mw_dept_mapping` as b
on a.acctg_dept_nbr = b.acctg_dept_nbr
where wm_mth_nbr=  1 --update
group by 1, 2, 3) as b
on a.sbu = b.sbu
and a.department = b.department
and a.channel = b.channel
