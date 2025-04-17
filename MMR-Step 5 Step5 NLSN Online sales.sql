
create or replace TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_5p_sales_FY26Mar_NLSN` --UPDATE  
AS
(SELECT catlg_item_id, a.div_id,div_desc,a.super_dept_id,super_dept_desc,a.dept_id,dept_desc,a.categ_id,category_desc,a.sub_categ_id,subcategory_desc,upc,gtin,wupc,prod_nm,prod_type_nm,wm_year_nbr,wm_month_num_of_yr,mmr_hier_id,watch,sub_cat_excl,S2H_Sales_Net,S2S_Sales_Net,PUT_Sales_Net,SFS_Sales_Net,P4_Sales_Net,S2H_Units_Net,S2S_Units_Net,PUT_Units_Net,SFS_Units_Net,P4_Units_Net,MP_Sales_Net,MP_Units_Net
FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_dotcom_5p_FY26Mar` a --UPDATE 
INNER JOIN (select DIV_ID,
DIV_DESC,
SUPER_DEPT_ID,
SUPER_DEPT_DESC,
DEPT_ID,
DEPT_DESC,
CATEGORY_ID,
CATEGORY_DESC,
SUBCATEGORY_ID,
SUBCATEGORY_DESC,
WATCH,
SUB_CAT_EXCL,
Dept_Excl,
Use_Nielsen_UPC,
NPD_mmr_id as mmr_hier_id from `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_online_subcat_mapping_FY26Mar`) b  #restatement
ON (a.div_id = b.div_id 
    AND a.super_dept_id = b.super_dept_id
    AND a.dept_id = b.dept_id
    AND a.categ_id = b.category_id
	AND a.sub_categ_id = b.subcategory_id)
GROUP BY catlg_item_id, a.div_id,div_desc,a.super_dept_id,super_dept_desc,a.dept_id,dept_desc,a.categ_id,category_desc,a.sub_categ_id,subcategory_desc,upc,gtin,wupc,prod_nm,prod_type_nm,wm_year_nbr,wm_month_num_of_yr,mmr_hier_id,watch,sub_cat_excl,S2H_Sales_Net,S2S_Sales_Net,PUT_Sales_Net,SFS_Sales_Net,P4_Sales_Net,S2H_Units_Net,S2S_Units_Net,PUT_Units_Net,SFS_Units_Net,P4_Units_Net,MP_Sales_Net,MP_Units_Net)
