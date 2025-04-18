CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Mar_NLSN` as --UPDATE 
SELECT p5_sales_group.prod_type_nm,
       # case when lookup_mmr.sub_cat_excl is null then "" else lookup_mmr.sub_cat_excl end AS prod_type_nm_excl,
       lookup_mmr.sub_cat_excl AS prod_type_nm_excl,
       false AS Watch,
       # case when lookup_mmr.mmr_heir_id is null then "" else lookup_mmr.mmr_heir_id end AS prod_type_nm_mmr_heir_id,
       lookup_mmr.mmr_heir_id AS prod_type_nm_mmr_heir_id,
       cast(CAST(date_sub(current_date(), interval 1 month) AS STRING FORMAT 'YYYYMM') as INT) as WM_Date_Added,
       cast(CAST(date_sub(current_date(), interval 1 month) AS STRING FORMAT 'YYYYMM') as INT) as mmr_rstmt_cycle

FROM
    (SELECT p5_sales_enhanced.prod_type_nm
     FROM
         (SELECT replace(a.prod_type_nm, '"', '') AS prod_type_nm,
                 a.prod_type_nm_excl,
                 a.Watch,
                 -- a.NLSN_mmr_id, -- update
                 a.prod_type_nm_mmr_heir_id as NLSN_mmr_id,
                 a.WM_Date_Added,
                 a.mmr_rstmt_cycle
          FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_fy25Aug_restatement_NLSN` -- UPDATE #restatement
         a) p5_sales
             RIGHT OUTER JOIN
         (SELECT b.catlg_item_id,
                 b.div_id,
                 b.div_desc,
                 b.super_dept_id,
                 b.super_dept_desc,
                 b.dept_id,
                 b.dept_desc,
                 b.categ_id,
                 b.category_desc,
                 b.sub_categ_id,
                 b.subcategory_desc,
                 b.upc,
                 b.gtin,
                 b.wupc,
                 b.prod_nm,
                 TRIM(b.prod_type_nm) AS prod_type_nm,
                 b.wm_year_nbr,
                 b.wm_month_num_of_yr,
                 b.mmr_hier_id,
                 b.watch,
                 b.sub_cat_excl,
                 b.S2H_Sales_Net,
                 b.S2S_Sales_Net,
                 b.PUT_Sales_Net,
                 b.SFS_Sales_Net,
                 b.P4_Sales_Net,
                 b.S2H_Units_Net,
                 b.S2S_Units_Net,
                 b.PUT_Units_Net,
                 b.SFS_Units_Net,
                 b.P4_Units_Net,
                 b.MP_Sales_Net,
                 b.MP_Units_Net
          FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_5p_sales_FY26Mar_NLSN` b -- UPDATE
          WHERE b.watch = TRUE
            AND prod_type_nm!='default' ) p5_sales_enhanced ON p5_sales.prod_type_nm=p5_sales_enhanced.prod_type_nm
     WHERE p5_sales.prod_type_nm IS NULL
     GROUP BY prod_type_nm) p5_sales_group
        LEFT JOIN
    (SELECT prod_type_nm,
            mmr_heir_id,
            sub_cat_excl,
            p5_sales_net,
            mmr_dept_id,
            mmr_category_group_id,
            mmr_category_id,
            mmr_heir_id AS right_mmr_heir_id,
            department_builder,
            reporting_level,
            business_unit,
            major_business,
            department,
            category_group,
            category
     FROM
         (SELECT p5_sales.prod_type_nm,
                 p5_sales.mmr_heir_id,
                 p5_sales.sub_cat_excl,
                 p5_sales.p5_sales_net,
                 p5_sales_enhanced.mmr_dept_id,
                 p5_sales_enhanced.mmr_category_group_id,
                 p5_sales_enhanced.mmr_category_id,
                 p5_sales_enhanced.mmr_hier_id AS right_mmr_heir_id,
                 p5_sales_enhanced.department_builder,
                 p5_sales_enhanced.reporting_level,
                 p5_sales_enhanced.business_unit,
                 p5_sales_enhanced.major_business,
                 p5_sales_enhanced.department,
                 p5_sales_enhanced.category_group,
                 p5_sales_enhanced.category,
                 row_number() over(PARTITION BY prod_type_nm
                               ORDER BY p5_sales_net DESC) rnk
          FROM
              (SELECT b.prod_type_nm,
                      b.mmr_hier_id AS mmr_heir_id,
                      b.sub_cat_excl,
                      
                     (Sum(Coalesce(b.P4_Sales_Net,0)) + Sum(Coalesce(b.MP_Sales_Net,0))) AS p5_sales_net
               FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_5p_sales_FY26Mar_NPD` b --UPDATE
               WHERE b.mmr_hier_id not in ('Parse')
               GROUP BY b.prod_type_nm,
                        b.mmr_hier_id,
                        b.sub_cat_excl
                        ) p5_sales
                  INNER JOIN
              (SELECT mmr_dept_id,
                      mmr_category_group_id,
                      mmr_category_id,
                      mmr_hier_id,
                      department_builder,
                      reporting_level,
                      MMR_SBU as business_unit,
MMR_MAJOR_BUSINESS as major_business,
MMR_DEPT as department,
MMR_CATEGORY_GROUP as category_group,
MMR_CATEGORY as category
               FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_mmr_hier_id_restatement_FY26Feb_NLSN`) p5_sales_enhanced ON p5_sales.mmr_heir_id = p5_sales_enhanced.mmr_hier_id --update
          WHERE right(mmr_heir_id, 4)!='9999' ) -- ##prev restatement
     WHERE rnk=1
     ORDER BY prod_type_nm) lookup_mmr ON p5_sales_group.prod_type_nm=lookup_mmr.prod_type_nm order by prod_type_nm;



 CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Mar_NPD` as --UPDATE 
SELECT p5_sales_group.prod_type_nm,
       # case when lookup_mmr.sub_cat_excl is null then "" else lookup_mmr.sub_cat_excl end AS prod_type_nm_excl,
       lookup_mmr.sub_cat_excl AS prod_type_nm_excl,
       false AS Watch,
       # case when lookup_mmr.mmr_heir_id is null then "" else lookup_mmr.mmr_heir_id end AS prod_type_nm_mmr_heir_id,
       lookup_mmr.mmr_heir_id AS prod_type_nm_mmr_heir_id,
       cast(CAST(date_sub(current_date(), interval 1 month) AS STRING FORMAT 'YYYYMM') as INT) as WM_Date_Added,
       cast(CAST(date_sub(current_date(), interval 1 month) AS STRING FORMAT 'YYYYMM') as INT) as mmr_rstmt_cycle

FROM
    (SELECT p5_sales_enhanced.prod_type_nm
     FROM
         (SELECT replace(a.prod_type_nm, '"', '') AS prod_type_nm,
                 a.prod_type_nm_excl,
                 a.Watch,
                 --a.NLSN_mmr_id, -- update
                  a.prod_type_nm_mmr_heir_id as NPD_mmr_id,
                 a.WM_Date_Added,
                 a.mmr_rstmt_cycle
          FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_fy25Aug_restatement_NPD` -- UPDATE #restatement
         a) p5_sales
             RIGHT OUTER JOIN
         (SELECT b.catlg_item_id,
                 b.div_id,
                 b.div_desc,
                 b.super_dept_id,
                 b.super_dept_desc,
                 b.dept_id,
                 b.dept_desc,
                 b.categ_id,
                 b.category_desc,
                 b.sub_categ_id,
                 b.subcategory_desc,
                 b.upc,
                 b.gtin,
                 b.wupc,
                 b.prod_nm,
                 TRIM(b.prod_type_nm) AS prod_type_nm,
                 b.wm_year_nbr,
                 b.wm_month_num_of_yr,
                 b.mmr_hier_id,
                 b.watch,
                 b.sub_cat_excl,
                 b.S2H_Sales_Net,
                 b.S2S_Sales_Net,
                 b.PUT_Sales_Net,
                 b.SFS_Sales_Net,
                 b.P4_Sales_Net,
                 b.S2H_Units_Net,
                 b.S2S_Units_Net,
                 b.PUT_Units_Net,
                 b.SFS_Units_Net,
                 b.P4_Units_Net,
                 b.MP_Sales_Net,
                 b.MP_Units_Net
          FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_5p_sales_FY26Mar_NPD` b -- UPDATE
          WHERE b.watch = TRUE
            AND prod_type_nm!='default' ) p5_sales_enhanced ON p5_sales.prod_type_nm=p5_sales_enhanced.prod_type_nm
     WHERE p5_sales.prod_type_nm IS NULL
     GROUP BY prod_type_nm) p5_sales_group
        LEFT JOIN
    (SELECT prod_type_nm,
            mmr_heir_id,
            sub_cat_excl,
            p5_sales_net,
            mmr_dept_id,
            mmr_category_group_id,
            mmr_category_id,
            mmr_heir_id AS right_mmr_heir_id,
            department_builder,
            reporting_level,
            business_unit,
            major_business,
            department,
            category_group,
            category
     FROM
         (SELECT p5_sales.prod_type_nm,
                 p5_sales.mmr_heir_id,
                 p5_sales.sub_cat_excl,
                 p5_sales.p5_sales_net,
                 p5_sales_enhanced.mmr_dept_id,
                 p5_sales_enhanced.mmr_category_group_id,
                 p5_sales_enhanced.mmr_category_id,
                 p5_sales_enhanced.mmr_hier_id AS right_mmr_heir_id,
                 p5_sales_enhanced.department_builder,
                 p5_sales_enhanced.reporting_level,
                 p5_sales_enhanced.business_unit,
                 p5_sales_enhanced.major_business,
                 p5_sales_enhanced.department,
                 p5_sales_enhanced.category_group,
                 p5_sales_enhanced.category,
                 row_number() over(PARTITION BY prod_type_nm
                               ORDER BY p5_sales_net DESC) rnk
          FROM
              (SELECT b.prod_type_nm,
                      b.mmr_hier_id AS mmr_heir_id,
                      b.sub_cat_excl,
                      
                     (Sum(Coalesce(b.P4_Sales_Net,0)) + Sum(Coalesce(b.MP_Sales_Net,0))) AS p5_sales_net
               FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_5p_sales_FY26Mar_NPD` b --UPDATE
               WHERE b.mmr_hier_id not in ('Parse')
               GROUP BY b.prod_type_nm,
                        b.mmr_hier_id,
                        b.sub_cat_excl
                        ) p5_sales
                  INNER JOIN
              (SELECT mmr_dept_id,
                      mmr_category_group_id,
                      mmr_category_id,
                      mmr_hier_id,
                      department_builder,
                      reporting_level,
                      MMR_SBU as business_unit,
MMR_MAJOR_BUSINESS as major_business,
MMR_DEPT as department,
MMR_CATEGORY_GROUP as category_group,
MMR_CATEGORY as category
               FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_mmr_hier_id_restatement_FY26Feb_NPD`) p5_sales_enhanced ON p5_sales.mmr_heir_id = p5_sales_enhanced.mmr_hier_id --update
          WHERE right(mmr_heir_id, 4)!='9999' ) -- ##prev restatement
     WHERE rnk=1
     ORDER BY prod_type_nm) lookup_mmr ON p5_sales_group.prod_type_nm=lookup_mmr.prod_type_nm order by prod_type_nm;

     create or replace table wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Mar as
select a.prod_type_nm,
a.prod_type_nm_excl,
a.Watch,
a.WM_Date_Added,
a.mmr_rstmt_cycle,
a.prod_type_nm_mmr_heir_id as NPD_mmr_id,
b.prod_type_nm_mmr_heir_id as NLSN_mmr_id
from wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Mar_NLSN as a
inner join wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Mar_NPD as b
on a.prod_type_nm = b.prod_type_nm;


INSERT INTO `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Mar` -- UPDATE
SELECT prod_type_nm,
prod_type_nm_excl,
Watch,
WM_Date_Added,
mmr_rstmt_cycle,
NLSN_MMR_ID,
NPD_MMR_ID
 FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Feb`; -- UPDATE
