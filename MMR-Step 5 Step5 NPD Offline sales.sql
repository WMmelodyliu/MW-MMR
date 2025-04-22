create or replace TABLE wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_scan_FY26Mar_NPD as -- UPDATE 
(
select financial_rpt_code,financial_rpt_desc,x.UPC_NBR,x.item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,
DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,
x.ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,x.DEPT_SUBCATG_DESC, x.DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, x.DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, 
MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, 
SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,
wm_year_nbr, wm_month_nbr,  wm_week_nbr, visit_date, visit_type, visit_subtype_code, dept_excl, sub_cat_excl, alt_mapping, 
CASE WHEN n.department_builder='NLSN' OR mmr_hier_id='Parse' THEN 'Y' ELSE 'N' END as use_nielsen_upc, 
mmr_hier_id,	r_mmr_hier_id, override_mmr_hier_id, upc_cd, total_sales, total_units 
FROM
(
select financial_rpt_code,financial_rpt_desc,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,wm_year_nbr, wm_month_nbr,  wm_week_nbr, visit_date, visit_type, visit_subtype_code, dept_excl, sub_cat_excl, alt_mapping, use_nielsen_upc, NPD_mmr_id as mmr_hier_id,	total_sales, total_units 
from `wmt-mint-mmr-mw-prod.MMR_numerator.wm_scan_FY26Mar` a --UPDATE #restatement
inner join `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_offline_subcat_mapping_FY26Mar` b -- UPDATE #restatement
on (a.acctg_dept_nbr = b.wm_acctg_dept_nbr 
    and a.dept_catg_grp_nbr = b.wm_dept_catg_grp_nbr
    and a.dept_category_nbr = b.wm_dept_category_nbr
    and a.dept_subcatg_nbr = b.wm_dept_subcatg_nbr)
--where a.dept_subcatg_nbr is not null and b.wm_dept_subcatg_nbr is not null
group by financial_rpt_code,financial_rpt_desc,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,wm_year_nbr, wm_month_nbr,  wm_week_nbr, visit_date, visit_type, visit_subtype_code, dept_excl, sub_cat_excl, alt_mapping, use_nielsen_upc, NPD_mmr_id,	total_sales, total_units 
UNION ALL
select financial_rpt_code,financial_rpt_desc,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,wm_year_nbr, wm_month_nbr,  wm_week_nbr, visit_date, visit_type, visit_subtype_code, dept_excl, sub_cat_excl, alt_mapping, use_nielsen_upc, NPD_mmr_id as mmr_hier_id,	total_sales, total_units 
from `wmt-mint-mmr-mw-prod.MMR_numerator.wm_scan_FY26Mar` a --UPDATE_TABLE 
inner join `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_offline_subcat_mapping_FY26Mar` b -- UPDATE #restatement
on a.acctg_dept_nbr = b.wm_acctg_dept_nbr 
where a.dept_subcatg_nbr is null and b.wm_dept_subcatg_nbr is null
group by financial_rpt_code,financial_rpt_desc,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,wm_year_nbr, wm_month_nbr,  wm_week_nbr, visit_date, visit_type, visit_subtype_code, dept_excl, sub_cat_excl, alt_mapping, use_nielsen_upc, NPD_mmr_id,	total_sales, total_units 
) x

left join
(select upc_nbr, item_nbr, override_mmr_hier_id from `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_upc_overrides`) as o
ON o.upc_nbr = x.upc_nbr and o.item_nbr = x.item_nbr

left join 
(select upc as upc_cd, max(t1.mmr_hier_id) as r_mmr_hier_id, max(department_builder) as department_builder 
	from 
	(
		select * from `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_nielsen_upc_mapping_FY26Mar` -- UPDATE ## delay at nielsen for upc in aug, hence using prev month upc
-- lkp_nielsen_upc_mapping_fy25Feb_new

		where mmr_hier_id not in ('MMR000000','MMR510000')
 	) t1
	join `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_mmr_hier_id_restatement_FY26Feb_NPD` t2 -- UPDATE
	on t1.mmr_hier_id = t2.mmr_hier_id
 group by upc_cd
) as n 
ON x.upc_nbr = n.upc_cd

Group by financial_rpt_code,financial_rpt_desc,x.UPC_NBR,x.item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC, MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,x.ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,x.DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, x.DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, x.DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,wm_year_nbr, wm_month_nbr, wm_week_nbr, visit_date, visit_type, visit_subtype_code, dept_excl, sub_cat_excl, alt_mapping, 
CASE WHEN n.department_builder='NLSN' OR mmr_hier_id='Parse' THEN 'Y' ELSE 'N' END, mmr_hier_id,	r_mmr_hier_id, override_mmr_hier_id, upc_cd, total_sales, total_units )
