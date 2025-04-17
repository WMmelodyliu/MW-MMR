create or replace table `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_numerator_FY26Mar_no53RE` as -- update name
-- step1 offline d82 sales
with d82 as (
select 
-- time
wm_full_yr_nbr,
wm_mth_nbr,
-- mw hierarchy
c.acctg_dept_nbr,
ifnull(c.catg_grp_desc, "UNASSIGNED") as catg_grp_desc,
ifnull(c.catg_desc, "UNASSIGNED") as catg_desc,
"UNASSIGNED" as subcatg_desc,
-- channel
"BIS" as channel,
"BIS" as channel_detail,
-- flag
"N/A" as `3P_FLAG`,
"N/A" as `PICKUP_FLAG`,
-- breakout
store_breakout,
brand_breakout,
-- sales
sum(total_sales) as wm_sales
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.offline_sales_FY26Mar_breakout_no53` as a -- update name -- sales, wmt hierarchy
inner join `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_nielsen_upc_mapping_FY26Feb44` as b -- update name -- nlsn upc, upc to mmr id
on a.upc_nbr = b.upc
and acctg_dept_nbr = 82 -- filter for d82 sales
inner join `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_d82_mapping` as c -- d82 mapping, mmr id to mw hierarchy
on b.mmr_hier_id = c.mmr_hier_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),
-- step 2 offline non d82 sales
non_d82 as (
select 
-- time
wm_full_yr_nbr,
wm_mth_nbr,
-- mw hierarchy
acctg_dept_nbr,
ifnull(dept_catg_grp_desc, "UNASSIGNED") as catg_grp_desc,
ifnull(dept_catg_desc, "UNASSIGNED") as catg_desc,
ifnull(dept_subcatg_desc, "UNASSIGNED") as subcatg_desc,
-- channel
"BIS" as channel,
"BIS" as channel_detail,
-- flag
"N/A" as `3P_FLAG`,
"N/A" as `PICKUP_FLAG`,
-- breakout
store_breakout,
brand_breakout,
-- sales
sum(total_sales) as wm_sales
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.offline_sales_FY26Mar_breakout_no53` -- update name -- sales, wmt hierarchy
where acctg_dept_nbr != 82 -- filter for non d82 sales
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),
-- step3 online sales
online as (
select 
-- time
wm_full_yr_nbr,
wm_mth_nbr,
-- mw hierarchy
acctg_dept_nbr,
ifnull(dept_catg_grp_desc, "UNASSIGNED") as catg_grp_desc,
ifnull(dept_catg_desc, "UNASSIGNED") as catg_desc,
ifnull(dept_subcatg_desc, "UNASSIGNED") as subcatg_desc,
-- channel
"Online" as channel,
case when chnl = "Delivery" then "Scheduled Delivery"
when chnl = "Pickup" then "Scheduled Pickup"
when chnl = "S2H" then "Core" else chnl end as channel_detail,
-- flag
case when chnl = "MP" then "Y" else "N" end as `3P_FLAG`,
case when chnl in ("PUT", "Pickup") then "Y" else "N" end as `PICKUP_FLAG`,
-- breakout
"N/A" as store_breakout,
brand_breakout,
-- sales
sum(gmv_actl) as wm_sales
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.online_sales_FY26Mar_breakout_no53` -- update name -- sales, wmt hierarchy
where chnl in ("Delivery", "Pickup", "PUT", "S2H", "SFS", "MP") -- filter for online channels
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
),
-- step 4 union online and offline sales
sales as (
select *
from d82
union all
select *
from non_d82
union all
select *
from online
),
-- step 5 total sales
total_sales as(
select *
from sales
union all
select 
-- time
wm_full_yr_nbr,
wm_mth_nbr,
-- mw hierarchy
acctg_dept_nbr,
catg_grp_desc,
catg_desc,
subcatg_desc,
-- channel
"Total" as channel,
"Total" as channel_detail,
-- flag
"N/A" as `3P_FLAG`,
"N/A" as `PICKUP_FLAG`,
-- breakout
"N/A" as store_breakout,
brand_breakout,
-- sales
sum(wm_sales) as wm_sales
from sales
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)
-- step 6 adjustment and output
select 
-- time
concat("FY", right(cast(wm_full_yr_nbr + 1 as string), 2)) as fiscal_year,
wm_full_yr_nbr as wm_year_nbr,
wm_mth_nbr as wm_month_nbr,
case when wm_mth_nbr = 12 then "January"
when wm_mth_nbr = 1 then "February"
when wm_mth_nbr = 2 then "March"
when wm_mth_nbr = 3 then "April"
when wm_mth_nbr = 4 then "May"
when wm_mth_nbr = 5 then "June"
when wm_mth_nbr = 6 then "July"
when wm_mth_nbr = 7 then "August"
when wm_mth_nbr = 8 then "September"
when wm_mth_nbr = 9 then "October"
when wm_mth_nbr = 10 then "November"
when wm_mth_nbr = 11 then "December"
end as wm_month_desc,
case when wm_mth_nbr = 12 then wm_full_yr_nbr + 1 else wm_full_yr_nbr end as cal_year_nbr,
case when wm_mth_nbr = 12 then 1 else wm_mth_nbr + 1 end as cal_month_nbr,
-- mw hierarchy
-- moved
case when moved is true then c.new_SBU else b.SBU end as SBU,
case when moved is true then c.new_BU else b.BU end as BU,
case when moved is true then c.new_department else b.department end as department,
-- unassigned
case when a.acctg_dept_nbr = 23 and moved is null then ifnull(d.new_catg_grp_desc, "Unknown")
when unassigned_catg_grp is true then "UNASSIGNED" else a.catg_grp_desc end as catg_grp_desc,
case when a.acctg_dept_nbr = 23 and moved is null then ifnull(d.new_catg_desc, "Unknown")
when unassigned_catg is true then "UNASSIGNED" else a.catg_desc end as catg_desc,
-- channel
channel,
channel_detail,
-- flag
`3P_FLAG`,
`PICKUP_FLAG`,
-- breakout
store_breakout,
brand_breakout,
-- sales
sum(wm_sales) as wm_sales
from total_sales as a
inner join `wmt-mint-mmr-mw-prod.new_mw_numerator_dev.mw_dept_mapping` as b
on a.acctg_dept_nbr = b.acctg_dept_nbr
left join `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_hierarchy_adjustment_FY26MarRE` as c -- update
on a.acctg_dept_nbr = c.acctg_dept_nbr
and a.catg_grp_desc = c.catg_grp_desc
and a.catg_desc = c.catg_desc
and a.subcatg_desc = c.subcatg_desc
left join `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_d23_mapping` as d
on a.acctg_dept_nbr = d.acctg_dept_nbr
and a.catg_grp_desc = d.catg_grp_desc
and a.catg_desc = d.catg_desc
and a.subcatg_desc = d.subcatg_desc
where excluded is not true
or excluded is null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17;

