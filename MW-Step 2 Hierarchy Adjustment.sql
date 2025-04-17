-- 2a full hierarchy
-- hierarchy = offline + online + history
create or replace table `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_hierarchy_FY26Feb` as -- update name
select distinct *
from (select distinct acctg_dept_nbr, acctg_dept_desc, dept_catg_grp_nbr, dept_catg_grp_desc, dept_catg_nbr, dept_catg_desc, dept_subcatg_nbr, dept_subcatg_desc
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.offline_sales_FY26Feb_breakout` -- update name
union distinct
select distinct acctg_dept_nbr, acctg_dept_desc, dept_catg_grp_nbr, dept_catg_grp_desc, dept_catg_nbr, dept_catg_desc, dept_subcatg_nbr, dept_subcatg_desc
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.online_sales_FY26Feb_breakout`) -- update name
union distinct
select *
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_hierarchy_FY25Jan`; -- update name

-- 2b new hierarchy and adjustment
-- new hierarchy = hierarchy - history hierarchy
with hierarchy as (
select distinct sbu, bu, department, a.acctg_dept_nbr, catg_grp_desc, catg_desc, subcatg_desc
from (select distinct acctg_dept_nbr, dept_catg_grp_nbr as catg_grp_nbr, dept_catg_nbr as catg_nbr, dept_subcatg_nbr as subcatg_nbr, dept_catg_grp_desc as catg_grp_desc, dept_catg_desc as catg_desc, dept_subcatg_desc as subcatg_desc
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_hierarchy_FY26Feb` -- update name
except distinct
select distinct acctg_dept_nbr, dept_catg_grp_nbr as catg_grp_nbr, dept_catg_nbr as catg_nbr, dept_subcatg_nbr as subcatg_nbr, dept_catg_grp_desc as catg_grp_desc, dept_catg_desc as catg_desc, dept_subcatg_desc as subcatg_desc
from `wmt-mint-mmr-mw-prod.mw_numerator_dev.mw_hierarchy_FY25Jan` -- update name
) as a
inner join `wmt-mint-mmr-mw-prod.new_mw_numerator_dev.mw_dept_mapping` as b
on a.acctg_dept_nbr = b.acctg_dept_nbr
),
-- excluded
hierarchy_excluded as (
select *,
case when concat(catg_grp_desc, catg_desc, subcatg_desc) like "%B2B%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%CVP%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%DELETE%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%FEES%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%FIREWORKS%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%PREPAID%AIRTIME%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%PROOANE%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%SERVICE%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%ALASKA%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%HAWAII%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%PUERTO RICO%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%(AK)%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%(HI)%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%(PR)%"
or concat(catg_grp_desc, catg_desc, subcatg_desc) like "%AK%HI%PR%"
or catg_grp_desc in ("AK", "HI", "PR")
or catg_desc in ("AK", "HI", "PR")
or subcatg_desc in ("AK", "HI", "PR")
or catg_grp_desc like "AK %" or catg_desc like "AK %" or subcatg_desc like "AK %"
or catg_grp_desc like "HI %" or catg_desc like "HI %" or subcatg_desc like "HI %"
or catg_grp_desc like "PR %" or catg_desc like "PR %" or subcatg_desc like "PR %"
or catg_grp_desc like "AK-%" or catg_desc like "AK-%" or subcatg_desc like "AK-%"
or catg_grp_desc like "HI-%" or catg_desc like "HI-%" or subcatg_desc like "HI-%"
or catg_grp_desc like "PR-%" or catg_desc like "PR-%" or subcatg_desc like "PR-%"
then TRUE
end as excluded
from hierarchy),
-- moved
hierarchy_moved as (
select *, case when new_acctg_dept_nbr is not null then true end as moved
from (select *,
case
-- stationary to seasonal and celebrantion
-- party
when acctg_dept_nbr = 3
and (catg_grp_desc like "%PARTY%" or catg_desc like "%PARTY%" or subcatg_desc like "%PARTY%") 
then 67
-- other to automotive
when acctg_dept_nbr != 10
and (catg_grp_desc like "%AUTO%" or catg_desc like "%AUTO%" or subcatg_desc like "%AUTO%") 
then 10
-- others to infant
when SBU != "Apparel" and SBU != "Food" and acctg_dept_nbr not in (79, 7, 18, 67)
and (catg_grp_desc like "%BABY%" or catg_desc like "%BABY%" or subcatg_desc like "%BABY%" or catg_grp_desc like "%INFANT%" or catg_desc like "%INFANT%" or subcatg_desc like "%INFANT%") 
then 79
-- personal care to beauty
-- suncare / trial and travel / haircare
when acctg_dept_nbr = 2
and (catg_grp_desc like "%SUNCARE%" or catg_desc like "%SUNCARE%" or subcatg_desc like "%SUNCARE%" 
or catg_grp_desc like "%TRIAL%TRAVEL%" or catg_desc like "%TRIAL%TRAVEL%" or subcatg_desc like "%TRIAL%TRAVEL%"
or catg_grp_desc like "%HAIR%" or catg_desc like "%HAIR%" or subcatg_desc like "%HAIR%")
then 46
-- sporting goods to cook and dine
-- tumbler
when acctg_dept_nbr = 9
and (catg_grp_desc like "%TUMBLER%" or catg_desc like "%TUMBLER%" or subcatg_desc like "%TUMBLER%") 
then 14
-- cook and dine to home management
-- food storage
when acctg_dept_nbr = 14
and (catg_grp_desc like "%FOOD%STORAGE%" or catg_desc like "%FOOD%STORAGE%" or subcatg_desc like "%FOOD%STORAGE%") 
then 74
-- dairy to snacks and beverages
-- chilled beverages
when acctg_dept_nbr = 90
and (catg_grp_desc like "%CHILLED%BEVERAGES%" or catg_desc like "%CHILLED%BEVERAGES%" or subcatg_desc like "%CHILLED%BEVERAGES%") 
then 95
end as new_acctg_dept_nbr
from hierarchy_excluded)),
-- unassigned
hierarchy_unassigned as (
select *,
case when catg_grp_desc like "%UNASSIGNED%" or catg_grp_desc like "%UNKNOWN%" or catg_grp_desc like "%MISC%" or catg_grp_desc like "%DOTCOM%" or catg_grp_desc IS NULL then TRUE end as unassigned_catg_group,
case when catg_desc like "%UNASSIGNED%" or catg_desc like "%UNKNOWN%" or catg_desc like "%MISC%" or catg_desc like "%DOTCOM%" or catg_desc IS NULL then TRUE end as unassigned_catg,
case when subcatg_desc like "%UNASSIGNED%" or subcatg_desc like "%UNKNOWN%" or subcatg_desc like "%MISC%" or subcatg_desc like "%DOTCOM%" or subcatg_desc IS NULL then TRUE end as unassigned_sub_catg,
from hierarchy_moved
)
select distinct *
from hierarchy_unassigned;
