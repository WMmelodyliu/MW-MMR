
## created a table to fetch offline sales - wm_scan
CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.wm_scan_FY26Mar` as --UPDATE
(	SELECT FINANCIAL_RPT_CODE,FINANCIAL_RPT_DESC,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,
	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, 
	DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, 
	MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, 
	PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,WM_YEAR_NBR, WM_MONTH_NBR, WM_WEEK_NBR, VISIT_DATE, VISIT_TYPE, VISIT_SUBTYPE_CODE, 
	SUM(SALES) AS total_sales, SUM(UNITS) AS total_units, COUNT(*) AS total_visits
	FROM 
		(
		SELECT FINANCIAL_RPT_CODE,FINANCIAL_RPT_DESC,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC
		,MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC
		, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR
		, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC
		, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,WM_YEAR_NBR, WM_MONTH_NBR, WM_WEEK_NBR, VISIT_DATE, VISIT_TYPE, VISIT_SUBTYPE_CODE, 
		SUM(RETAIL_PRICE) AS sales, SUM(UNIT_QTY) AS units --all belongs to x table
		FROM 
			(
			SELECT UPC_NBR,ITEM_NBR,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC
			,MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC
			, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR
			, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC
			, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC 
			FROM `wmt-edw-prod.WW_CORE_DIM_VM.ITEM_DIM` WHERE current_ind = "Y"
			AND base_div_nbr = 1
			AND country_code='US'
			GROUP BY UPC_NBR,ITEM_NBR,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC
			,MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC
			, DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR
			, MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC
			, PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC
			) AS a --item table: this table brings in the active items
		INNER JOIN 
			(
			SELECT a1.STORE_NBR, FINANCIAL_RPT_CODE, FINANCIAL_RPT_DESC, SCAN_ID, visit_date, visit_type, visit_subtype_code, SUM(retail_price) AS retail_price, SUM(unit_qty) AS unit_qty
			FROM
				(SELECT STORE_NBR, FINANCIAL_RPT_CODE, FINANCIAL_RPT_DESC
				FROM  `wmt-edw-prod.WW_CORE_DIM_VM.STORE_DIM`
				WHERE  COUNTRY_CODE = "US"
				AND BASE_DIV_NBR = 1 --Walmart only
				AND CURRENT_IND = "Y"
				AND STATE_PROV_CODE NOT IN('PR')
				GROUP BY STORE_NBR, FINANCIAL_RPT_CODE, FINANCIAL_RPT_DESC) a1  --this is to limit to only US stores
			INNER JOIN
				(SELECT s.STORE_NBR, s.SCAN_ID, s.VISIT_DATE, v.VISIT_TYPE, v.VISIT_SUBTYPE_CODE, SUM(RETAIL_PRICE) AS retail_price, SUM(UNIT_QTY) AS unit_qty
				FROM `wmt-edw-prod.US_WM_MB_VM.SCAN` s
				INNER JOIN `wmt-edw-prod.US_WM_MB_VM.VISIT` v 
				ON v.VISIT_NBR = s.VISIT_NBR AND v.STORE_NBR = s.STORE_NBR  AND v.VISIT_DATE=s.VISIT_DATE
				WHERE SCAN_TYPE =0		
				AND s.OTHER_INCOME_IND IS NULL
				AND s.VISIT_DATE>='2024-12-28' AND s.VISIT_DATE<='2025-04-04' --UPDATE
				GROUP BY s.STORE_NBR, s.SCAN_ID, s.VISIT_DATE, v.VISIT_TYPE, v.VISIT_SUBTYPE_CODE) b
				ON a1.STORE_NBR = b.STORE_NBR
				GROUP BY a1.STORE_NBR, FINANCIAL_RPT_CODE, FINANCIAL_RPT_DESC, b.SCAN_ID, VISIT_DATE, VISIT_TYPE, VISIT_SUBTYPE_CODE
				) AS b --store sales table: bringing in US store sales
			ON a.MDS_FAM_ID = b.SCAN_ID 
		INNER JOIN 
			(SELECT CALENDAR_DATE, WM_MONTH_NBR, WM_MONTH_NAME, WM_YEAR_NBR, WM_WEEK_NBR
			FROM `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM`
			-- WHERE WM_YEAR_NBR IN (2022, 2023) --UPDATE -- removed it so that we get all history data 
			) AS c -- Calendar table: Select "WM" Calendar year not fiscal
		ON b.VISIT_DATE = c.CALENDAR_DATE
	GROUP BY FINANCIAL_RPT_CODE,FINANCIAL_RPT_DESC,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,
	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, 
	DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, 
	MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, 
	PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,WM_YEAR_NBR, WM_MONTH_NBR, WM_WEEK_NBR, VISIT_DATE, VISIT_TYPE, VISIT_SUBTYPE_CODE
		) x
	GROUP BY  FINANCIAL_RPT_CODE,FINANCIAL_RPT_DESC,UPC_NBR,item_nbr,MDS_FAM_ID,ITEM1_DESC,BASE_DIV_NBR,COUNTRY_CODE,DEPT_NBR,DEPT_DESC,MDSE_CATG_NBR,MDSE_CATG_DESC,
	MDSE_SUBCATG_NBR,MDSE_SUBCATG_DESC,FINELINE_NBR,FINELINE_DESC,ACCTG_DEPT_NBR,ACCTG_DEPT_DESC,DEPT_SUBCATG_NBR,DEPT_SUBCATG_DESC, 
	DEPT_CATEGORY_NBR, DEPT_CATEGORY_DESC, DEPT_CATG_GRP_NBR, DEPT_CATG_GRP_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, MDSE_SEGMENT_NBR, 
	MDSE_SEGMENT_DESC, VENDOR_NBR, VENDOR_NAME, ITEM2_DESC, COLOR_DESC, SIZE_DESC, SHOP_DESC, SIGNING_DESC, UPC_DESC, PLU_NBR, CONSUMER_ITEM_DESC, 
	PRODUCT_DESC, ITEM_STATUS_CODE, ITEM_STATUS_DESC,WM_YEAR_NBR, WM_MONTH_NBR, WM_WEEK_NBR, VISIT_DATE, VISIT_TYPE, VISIT_SUBTYPE_CODE
)
