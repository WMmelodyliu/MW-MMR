CREATE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.FY26Mar_subcat_unmapped_online` AS
WITH dotcom_5p AS (SELECT wm_dotcom_base.div_id,
                 wm_dotcom_base.super_dept_id,
                 wm_dotcom_base.dept_id,
                 cast(wm_dotcom_base.categ_id AS bigint) AS categ_id,
                 wm_dotcom_base.sub_categ_id,
                 Sum(wm_dotcom_base.p4_sales_net) AS p4,
                 Sum(wm_dotcom_base.mp_sales_net) AS mp
          FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_dotcom_5p_FY26Mar` wm_dotcom_base -- update
          GROUP BY wm_dotcom_base.div_id,
                   wm_dotcom_base.super_dept_id,
                   wm_dotcom_base.dept_id,
                   wm_dotcom_base.categ_id,
                   wm_dotcom_base.sub_categ_id)

SELECT t3.div_id,
       t4.rpt_hrchy_lvl0_desc,
       t3.SUPER_DEPT_ID,
       t4.rpt_hrchy_lvl1_desc,
       t3.DEPT_ID,
       t4.rpt_hrchy_lvl2_desc,
       t3.categ_id,
       t4.rpt_hrchy_lvl3_desc,
       t3.sub_categ_id,
       t4.rpt_hrchy_lvl4_desc,
       "" as MMR_HIER_ID,
       FALSE as WATCH,
       case when t3.super_dept_id = 28000 and t3.categ_id = 1028059 then TRUE else FALSE end as SUB_CAT_EXCL,
       case when t3.super_dept_id IN (70100,20400) then TRUE else FALSE end as Dept_Excl,
       case when t4.rpt_hrchy_lvl0_desc like "%FOOD%" or rpt_hrchy_lvl0_desc like "%CONSUMABLES%" then TRUE else FALSE end as Use_Nielsen_UPC
FROM
    (SELECT dotcom_5p.*
      FROM dotcom_5p
      EXCEPT DISTINCT
      SELECT dotcom_5p.*
      FROM dotcom_5p 
      INNER JOIN
         (SELECT DIV_ID,
                 SUPER_DEPT_ID,
                 DEPT_ID,
                 CATEGORY_ID,
                 SUBCATEGORY_ID
          FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_online_subcat_mapping_FY26Mar`) lookup_online -- update
             ON IFNULL(dotcom_5p.DIV_ID, 00000000) =IFNULL(lookup_online.DIV_ID, 00000000)
             AND IFNULL(dotcom_5p.SUPER_DEPT_ID, 00000000)=IFNULL(lookup_online.SUPER_DEPT_ID, 00000000)
             AND IFNULL(dotcom_5p.dept_id, 00000000)=IFNULL(lookup_online.DEPT_ID, 00000000)
             AND IFNULL(dotcom_5p.categ_id, 00000000) =IFNULL(lookup_online.CATEGORY_ID, 00000000)
             AND IFNULL(dotcom_5p.sub_categ_id, 00000000)=IFNULL(lookup_online.SUBCATEGORY_ID, 00000000)) t3
        INNER JOIN
    (SELECT rh.rpt_hrchy_lvl0_id,
            rh.rpt_hrchy_lvl0_desc,
            rh.rpt_hrchy_lvl1_id,
            rh.rpt_hrchy_lvl1_desc,
            rh.rpt_hrchy_lvl2_id,
            rh.rpt_hrchy_lvl2_desc,
            rh.rpt_hrchy_lvl3_id,
            rh.rpt_hrchy_lvl3_desc,
            rh.rpt_hrchy_lvl4_id,
            rh.rpt_hrchy_lvl4_desc
FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.RPT_HRCHY` rh 

     GROUP BY 1,2,3,4,5,6,7,8,9,10) t4 ON t3.DIV_ID =t4.rpt_hrchy_lvl0_id
        AND t3.SUPER_DEPT_ID =t4.rpt_hrchy_lvl1_id
        AND t3.DEPT_ID =t4.rpt_hrchy_lvl2_id
        AND t3.categ_id =t4.rpt_hrchy_lvl3_id
        AND t3.sub_categ_id=t4.rpt_hrchy_lvl4_id
ORDER BY t3.div_id;
-- query using prev tables
-- FROM `wmt-edw-prod.WW_GEC_VM.PROD_RPT_HRCHY` rh 
--      WHERE 
--               rh.curr_ind = 1
--      GROUP BY rh.div_id,
--               rh.div_nm,
--               rh.super_dept_id,
--               rh.super_dept_nm,
--               rh.dept_id,
--               rh.dept_nm,
--               rh.categ_id,
--               rh.categ_nm,
--               rh.sub_categ_id,
--               rh.sub_categ_nm) t4 ON t3.DIV_ID =t4.div_id
--         AND t3.SUPER_DEPT_ID =t4.super_dept_id
--         AND t3.DEPT_ID =t4.dept_id
--         AND t3.categ_id =t4.categ_id
--         AND t3.sub_categ_id=t4.sub_categ_id
-- ORDER BY t3.div_id;
