## taking the scan table, making some case whens for excluding, incl flag and then inner joining with the lkp table ( except clause) to remove the already mapped line items and get the list of line items with blank MMR ID
CREATE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.FY26Mar_subcat_unmapped_offline` AS -- UPDATE
WITH wm_scan AS (SELECT wm_base.acctg_dept_nbr,
            wm_base.acctg_dept_desc,
            wm_base.dept_catg_grp_nbr,
            wm_base.dept_catg_grp_desc,
            wm_base.dept_category_nbr,
            wm_base.dept_category_desc,
            wm_base.dept_subcatg_nbr,
            wm_base.dept_subcatg_desc,
            Sum(wm_base.total_sales) AS total_sales
     FROM `wmt-mint-mmr-mw-prod.MMR_numerator.wm_scan_FY26Mar` wm_base -- UPDATE
     GROUP BY wm_base.acctg_dept_nbr,
              wm_base.acctg_dept_desc,
              wm_base.dept_catg_grp_nbr,
              wm_base.dept_catg_grp_desc,
              wm_base.dept_category_nbr,
              wm_base.dept_category_desc,
              wm_base.dept_subcatg_nbr,
              wm_base.dept_subcatg_desc)

SELECT acctg_dept_nbr,
       acctg_dept_desc,
       dept_catg_grp_nbr,
       dept_catg_grp_desc,
       dept_category_nbr,
       dept_category_desc,
       dept_subcatg_nbr,
       dept_subcatg_desc,
       total_sales,
       case when acctg_dept_nbr in (99,85,86) then TRUE else FALSE end as dept_excl,
       case when dept_subcatg_desc like '%CVP%' or dept_subcatg_desc like '%Alaska%' or dept_subcatg_desc like '%Hawaii%' or dept_subcatg_desc like '%Puerto Rico%' or dept_subcatg_nbr = 1070605 then TRUE else FALSE end as sub_cat_excl,
       FALSE as alt_mapping,
       case when acctg_dept_nbr in (1,80,81,82,90,91,92,93,94,95,96,97,98,2,4,8,13,40,46,79) then TRUE else FALSE end as use_nielsen_upc,
       FALSE as ogp_prefy18,
       
       "" as mmr_hier_id
       
FROM (
  SELECT *
  FROM wm_scan
  EXCEPT DISTINCT
SELECT wm_scan.*
FROM wm_scan
INNER JOIN (SELECT *
FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_offline_subcat_mapping_FY26Mar` lkp_offline_subcat_mapping) lookup_offline --UPDATE
ON IFNULL(wm_scan.acctg_dept_nbr, 000000000)=IFNULL(lookup_offline.wm_acctg_dept_nbr, 000000000)
AND IFNULL(wm_scan.dept_catg_grp_nbr, 000000000)=IFNULL(lookup_offline.wm_dept_catg_grp_nbr, 000000000)
AND IFNULL(wm_scan.dept_category_nbr, 000000000)=IFNULL(lookup_offline.wm_dept_category_nbr, 000000000)
AND IFNULL(wm_scan.dept_subcatg_nbr, 000000000)=IFNULL(lookup_offline.wm_dept_subcatg_nbr, 000000000)
)
;
