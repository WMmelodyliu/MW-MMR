
-- running to remove del ind and checking if it has any affect on the online sales for June
CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_dotcom_4p_FY26Mar` -- UPDATE
AS
(SELECT  
			mp.catlg_item_id, rh.div_id, rh.super_dept_id, rh.dept_id, rh.categ_id, rh.sub_categ_id, pil.UPC, pil.GTIN, pil.WUPC ,prod.prod_nm, 
			prod.prod_type_nm, cal_yr.wm_year_nbr, cal.wm_month_num_of_yr,
			 COALESCE(SUM(mp.S2H_Net_Sales),0) AS S2H_Sales_Net,
			 COALESCE(SUM(mp.S2S_Net_Sales),0) AS S2S_Sales_Net,
			 COALESCE(SUM(mp.PUT_Net_Sales),0) AS PUT_Sales_Net,
			 COALESCE(SUM(mp.SFS_Net_Sales),0) AS SFS_Sales_Net,
			 COALESCE(SUM(mp.P4_Net_Sales),0) AS P4_Sales_Net,
			 (COALESCE(SUM(mp.S2H_Gross_Units),0) - COALESCE(SUM(mp.S2H_Rfnd_Units),0)) AS S2H_Units_Net,
			 (COALESCE(SUM(mp.S2S_Gross_Units),0) - COALESCE(SUM(mp.S2S_Rfnd_Units),0)) AS S2S_Units_Net,
			 (COALESCE(SUM(mp.PUT_Gross_Units),0) - COALESCE(SUM(mp.PUT_Rfnd_Units),0)) AS PUT_Units_Net,
			 (COALESCE(SUM(mp.SFS_Gross_Units),0) - COALESCE(SUM(mp.SFS_Rfnd_Units),0)) AS SFS_Units_Net,
			 (COALESCE(SUM(mp.P4_Gross_Units),0) - COALESCE(SUM(mp.P4_Rfnd_Units),0)) AS P4_Units_Net			 
FROM		
	(
	SELECT 
	CASE 	WHEN s1.catlg_item_id IS NOT NULL THEN s1.catlg_item_id 
			ELSE rf1.catlg_item_id END AS catlg_item_id, 
	CASE 	WHEN s1.event_dt IS NOT NULL THEN s1.event_dt 
			ELSE rf1.event_dt END AS event_dt,
	CASE 	WHEN s1.shpg_node_org_cd IS NOT NULL THEN s1.shpg_node_org_cd 
			ELSE rf1.shpg_node_org_cd END AS shpg_node_org_cd,
	SUM(Site_to_Home) AS S2H_Net_Sales, 
	SUM(Ship_to_Store) AS S2S_Net_Sales, 
	SUM(Pick_up_Today) AS PUT_Net_Sales, 
	SUM(Ship_from_Store) AS SFS_Net_Sales,	
	SUM(Total_Sales) AS P4_Net_Sales,	
	SUM(Site_to_Home_Units) AS S2H_Gross_Units, 
	SUM(Ship_to_Store_Units) AS S2S_Gross_Units, 
	SUM(Pick_up_Today_Units) AS PUT_Gross_Units, 
	SUM(Ship_from_Store_Units) AS SFS_Gross_Units,	
	SUM(Total_Sales_Units) AS P4_Gross_Units,
	SUM(Site_to_Home_RfdUnits) AS S2H_Rfnd_Units, 
	SUM(Ship_to_Store_RfdUnits) AS S2S_Rfnd_Units, 
	SUM(Pick_up_Today_RfdUnits) AS PUT_Rfnd_Units, 
	SUM(Ship_from_Store_RfdUnits) AS SFS_Rfnd_Units,
	SUM(Total_Sales_RfdUnits) AS P4_Rfnd_Units

	FROM
		(SELECT s.catlg_item_id, s.event_dt, s.shpg_node_org_cd,
		SUM(CASE WHEN (s.svc_id = 0)THEN s.GROSS_SALES_REV_AMT - ABS(s.RFND_RTL_SALES_PHYS) - ABS(s.RFND_RTL_SALES_VIRT) + ABS(s.RCHRG_RTL_SALES_AMT) End)  AS Site_to_Home,
		SUM(CASE WHEN (s.svc_id = 8)THEN s.GROSS_SALES_REV_AMT - ABS(s.RFND_RTL_SALES_PHYS) - ABS(s.RFND_RTL_SALES_VIRT) + ABS(s.RCHRG_RTL_SALES_AMT) End)  AS Ship_to_Store,
		SUM(CASE WHEN (s.svc_id = 11)THEN s.GROSS_SALES_REV_AMT - ABS(s.RFND_RTL_SALES_PHYS) - ABS(s.RFND_RTL_SALES_VIRT) + ABS(s.RCHRG_RTL_SALES_AMT) End)  AS Pick_up_Today,
		SUM(CASE WHEN (s.svc_id = 14)THEN s.GROSS_SALES_REV_AMT - ABS(s.RFND_RTL_SALES_PHYS) - ABS(s.RFND_RTL_SALES_VIRT) + ABS(s.RCHRG_RTL_SALES_AMT) End)  AS Ship_from_Store,
		SUM(s.GROSS_SALES_REV_AMT - ABS(s.RFND_RTL_SALES_PHYS) - ABS(s.RFND_RTL_SALES_VIRT) + ABS(s.RCHRG_RTL_SALES_AMT)) AS Total_Sales,
		SUM(CASE WHEN (s.svc_id = 0)THEN s.SHPD_QTY End)  AS Site_to_Home_Units,
		SUM(CASE WHEN (s.svc_id = 8)THEN s.SHPD_QTY End)  AS Ship_to_Store_Units,
		SUM(CASE WHEN (s.svc_id = 11)THEN s.SHPD_QTY End)  AS Pick_up_Today_Units,
		SUM(CASE WHEN (s.svc_id = 14)THEN s.SHPD_QTY End)  AS Ship_from_Store_Units,
		SUM(s.SHPD_QTY) AS Total_Sales_Units
		-- FROM `wmt-edw-prod.WW_GEC_VM.ACCT_SUMM` s
    FROM `wmt-edw-prod.US_FIN_ECOMM_DL_VM.ACCT_SUMM` s --CHANGE
        WHERE s.EVENT_DT BETWEEN DATE '2024-12-28' AND DATE'2025-04-04'  --UPDATE DATE                    
        -- AND s.mart_org_id IN (0, 4571, 376515, 376518)                                     --Walmart.com, Walmart.com, Google Home
				AND s.mart_org_cd in ("0", "9") --CHANGE
        AND s.svc_id IN (0,8,11,14)                                           --S2H, S2S, PUT, SFS
				AND s.gift_card_ind=0                                 -- Exclude Gift Cards (different tab in AcSS)
        AND s.optical_ind=0                                 -- Exclude Glasses (different tab in AcSS)
		-- AND s.vertical_id = 0											--Remove OGP
		AND s.VERTICAL_NM in ("0","GM","4") --CHANGE
		GROUP BY 1,2,3
		) s1
	FULL  JOIN
		(SELECT rf.catlg_item_id, rf.event_dt, rf.shpg_node_org_cd,
		SUM(CASE WHEN (rf.svc_id = 0)THEN rf.REFUNDED_QTY End)  AS Site_to_Home_RfdUnits,
		SUM(CASE WHEN (rf.svc_id = 8)THEN rf.REFUNDED_QTY End)  AS Ship_to_Store_RfdUnits,
		SUM(CASE WHEN (rf.svc_id = 11)THEN rf.REFUNDED_QTY End)  AS Pick_up_Today_RfdUnits,
		SUM(CASE WHEN (rf.svc_id = 14)THEN rf.REFUNDED_QTY End)  AS Ship_from_Store_RfdUnits,
		SUM(rf.REFUNDED_QTY) AS Total_Sales_RfdUnits
		-- FROM `wmt-edw-prod.WW_GEC_RPT_VM.ACCT_FIN_RTN_SALES` rf
		FROM  `wmt-edw-prod.US_FIN_ECOMM_DL_RPT_VM.ACCT_FIN_RTN_SALES` rf --CHANGE
		WHERE rf.EVENT_DT  BETWEEN DATE '2024-12-28' AND DATE '2025-04-04' --UPDATE DATE
	    -- AND rf.tenant_org_id IN (0, 4571, 376515, 376518)                                     --Walmart.com, Walmart.com, Google Home
			  AND rf.op_cmpny_cd = "WMT.COM" ----CHANGE
        AND rf.svc_id IN (0,8,11,14)                                           --S2H, S2S, PUT, SFS
		AND rf.gift_card_ind=0                                 -- Exclude Gift Cards (different tab in AcSS)
        AND rf.optical_ind=0                                 -- Exclude Glasses (different tab in AcSS)
		-- AND rf.vertical_id = 0											--Remove OGP
		AND rf.VERTICAL_NM in ("0","GM","4") --CHANGE
		GROUP BY 1,2,3
		) rf1
	ON rf1.catlg_item_id = s1.catlg_item_id AND rf1.event_dt = s1.event_dt AND rf1.shpg_node_org_cd = s1.shpg_node_org_cd
	GROUP BY 1,2,3
	) mp

	JOIN     (
			   /*SELECT         t.catlg_item_id
			   FROM           `wmt-edw-prod.WW_GEC_VM.PROD_CLASS_TYPE_TRK` t
			   WHERE         t.actv_ind = 1  --？？？
			   AND          t.prod_class_type_id NOT IN (30,47,56)  -- GC Join to exclude*/
		-- 		 SELECT t.catlg_item_id
		-- 		 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD` t
		-- 		 WHERE DEL_IND = 0 --CHANGE
		-- 		 AND t.prod_class_type_id NOT IN (30,47,56) --CHANGE
		-- 	   ) t
    -- ON  mp.catlg_item_id = t.catlg_item_id   --> since the del ind flag is not correct
	SELECT catlg_item_id FROM (
	SELECT *,RANK() OVER (PARTITION BY catlg_item_id ORDER BY SRC_MODFD_TS DESC )  as recent_log
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD` t
				 WHERE 
				 t.prod_class_type_id NOT IN (30,47,56)
	 ) 
	 WHERE recent_log =1
	 )t
	ON mp.catlg_item_id = t.catlg_item_id            

	JOIN     (
			   /*SELECT         prh.rpt_hrchy_id,
						prh.catlg_item_id
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD_RPT_HRCHY` prh
			   WHERE         prh.curr_ind = 1                       --Item RHID --？？？
			   AND prh.catlg_item_id<> -999*/
				 SELECT prh.src_rpt_hrchy_id as rpt_hrchy_id, prh.catlg_item_id
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_RPT_HRCHY` prh
				 WHERE prh.catlg_item_id<> -999 --CHANGE
			   ) prh
	ON  mp.catlg_item_id = prh.catlg_item_id    
           
	JOIN     (
			   /*SELECT          rh.rpt_hrchy_id,rh.div_id, rh.super_dept_id, rh.dept_id, rh.categ_id, rh.sub_categ_id  
			   FROM         `wmt-edw-prod.WW_GEC_VM.RPT_HRCHY` rh
			   WHERE         rh.curr_ind = 1                        --RH ID Lookup*/ --？？？
				 SELECT rh.SRC_RPT_HRCHY_ID as rpt_hrchy_id,rh.rpt_hrchy_lvl0_id as div_id, rh.rpt_hrchy_lvl1_id as super_dept_id, rh.rpt_hrchy_lvl2_id as dept_id, rh.rpt_hrchy_lvl3_id as categ_id, rh.rpt_hrchy_lvl4_id as sub_categ_id
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.RPT_HRCHY` rh --CHANGE
			   ) rh
    ON  prh.rpt_hrchy_id = rh.rpt_hrchy_id         
------------------------------------------------------------------------------------          
    JOIN     (     
				/*SELECT catlg_item_id, MAX(upc) AS upc, MAX(GTIN) AS GTIN, MAX(WUPC) AS WUPC
				FROM
				(SELECT      pil.catlg_item_id,  
				CASE WHEN    pil.prod_id_type_id  = 2 THEN pil.prod_id_val END AS UPC,
				CASE WHEN    pil.prod_id_type_id  = 3 THEN pil.prod_id_val END AS GTIN,
				CASE WHEN    pil.prod_id_type_id  = 1 THEN pil.prod_id_val END AS WUPC
				FROM         `wmt-edw-prod.WW_GEC_VM.PROD_ID_LKP` pil
				WHERE        pil.prod_id_rank = 1                     --Remove Duplicate Items
				) a 
				GROUP BY catlg_item_id*/
				SELECT catlg_item_id, MAX(upc) AS upc, MAX(GTIN) AS GTIN, MAX(WUPC) AS WUPC
FROM
(SELECT pil.catlg_item_id,
CASE WHEN pil.PROD_ID_TYPE_NM = 'UPC' THEN pil.prod_id_val END AS UPC,
CASE WHEN pil.PROD_ID_TYPE_NM = 'GTIN' THEN pil.prod_id_val END AS GTIN,
CASE WHEN pil.PROD_ID_TYPE_NM = 'WUPC' THEN pil.prod_id_val END AS WUPC
FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_ALT_ID` pil
WHERE pil.PROD_ID_RANK_NBR = 1 --Remove Duplicate Items
) a
GROUP BY catlg_item_id
			   ) pil
    ON  mp.catlg_item_id = pil.catlg_item_id
	JOIN     (     
			   /*SELECT        prod.catlg_item_id, prod.prod_nm, prod.prod_type_id
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD` prod    */
				 SELECT prod.catlg_item_id, prod.prod_nm, prod.prod_type_nm
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD` prod        
			   ) prod
    ON  mp.catlg_item_id = prod.catlg_item_id
	
	/*JOIN     (     
			   SELECT        ptl.prod_type_id, ptl.prod_type_nm
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD_TYPE_LKP` ptl              
			   ) ptl
    ON  prod.prod_type_id = ptl.prod_type_id*/
------------------------------------------------------------------------------------   
     
	JOIN 	(                                          
			/*SELECT org_id, org_nm                                         
			FROM `wmt-edw-prod.WW_GEC_VM.ORG_SHIP_NODE` osn                                          
			WHERE src_org_key NOT IN ('297072','220815')						--Exclude Warranties, subscriptions
			AND TRIM(osn.org_nm) <>'Walmart.com Subscription'                 -- Exclude Shipping Pass Subscription*/
			SELECT distinct org_cd ---ISSUE!!!!!!!!!!!
			FROM `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.ORG_SHIP_NODE` osn --CHANGE
			WHERE src_org_key NOT IN ('297072','220815')
			AND TRIM(osn.org_nm) <>'Walmart.com Subscription'
			)  osn
    ON  mp.shpg_node_org_cd = osn.org_cd
	JOIN 	(                                          
			/*SELECT cal.wm_month_beg_dt, cal.wm_month_end_dt, cal.wm_month_num_of_yr, wm_yr_id                                         
			FROM `wmt-edw-prod.WW_GEC_VM.CAL_WM_MONTH` cal                      --GET WM MONTH NUMBER       */
			SELECT cal.wm_mth_beg_dt, cal.wm_mth_end_dt, cal.wm_mth_nbr_of_yr_nbr as wm_month_num_of_yr, wm_yr_id
			FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.CAL_WM_MONTH` cal --CHANGE
			)  cal
    ON  mp.EVENT_DT BETWEEN cal.wm_mth_beg_dt AND cal.wm_mth_end_dt
		JOIN 	(                                          
			/* SELECT RIGHT(cal_yr.wm_yr_desc,4) AS wm_year_nbr, cal_yr.wm_yr_id                                         
			FROM `wmt-edw-prod.WW_GEC_VM.CAL_WM_YR` cal_yr                     --GET WM Year Desc    */
			SELECT RIGHT(cal_yr.wm_yr_desc,4) AS wm_year_nbr, cal_yr.wm_yr_id
			FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.CAL_WM_YR` cal_yr --CHANGE    
			)  cal_yr
    ON  cal.wm_yr_id = cal_yr.wm_yr_id	 




GROUP BY mp.catlg_item_id, rh.div_id, rh.super_dept_id, rh.dept_id, rh.categ_id, rh.sub_categ_id, pil.UPC, pil.GTIN, pil.WUPC ,prod.prod_nm, 
			prod.prod_type_nm, cal_yr.wm_year_nbr, cal.wm_month_num_of_yr) --WITH DATA
--PRIMARY INDEX (catlg_item_id, div_id, super_dept_id, dept_id, categ_id, sub_categ_id, wm_year_nbr, wm_month_num_of_yr)
;

CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_dotcom_mp_FY26Mar` --UPDATE
AS
(SELECT  
			mp.catlg_item_id, rh.div_id, rh.super_dept_id, rh.dept_id, rh.categ_id, rh.sub_categ_id, pil.UPC, pil.GTIN, pil.WUPC ,prod.prod_nm, 
			prod.prod_type_nm, cal_yr.wm_year_nbr, cal.wm_month_num_of_yr,
			(COALESCE(SUM(mp.MP_Gross_Units),0) - COALESCE(SUM(mp.MP_Total_Units_Rfnd),0)) AS MP_Units_Net,
			(COALESCE(SUM(mp.MP_Gross_Sales),0) + COALESCE(SUM(mp.MP_Total_Rfnd),0)) AS MP_Sales_Net			 
FROM		
	(
	SELECT 
	CASE 	WHEN s2.catlg_item_id IS NOT NULL THEN s2.catlg_item_id 
			ELSE rf2.catlg_item_id END AS catlg_item_id, 
	CASE 	WHEN s2.event_dt IS NOT NULL THEN s2.event_dt 
			ELSE rf2.event_dt END AS event_dt,
	CASE 	WHEN s2.shpg_node_org_cd IS NOT NULL THEN s2.shpg_node_org_cd 
			ELSE rf2.shpg_node_org_cd END AS shpg_node_org_cd,
	SUM(Gross_Units) AS MP_Gross_Units, 
	SUM(Gross_Sales) AS MP_Gross_Sales, 
	SUM(Total_Units_Rfnd) AS MP_Total_Units_Rfnd, 
	SUM(Total_Rfnd) AS MP_Total_Rfnd

	FROM
		(SELECT s.catlg_item_id, s.event_dt, s.shpg_node_org_cd,
		--SUM(CAST(s.SHPD_QTY AS DEC(18,2))) AS Gross_Units,
        SUM(s.SHPD_QTY) AS Gross_Units, 		
		--SUM(CAST(s.ITEM_CHRG_AMT AS DEC(18,2))) AS Gross_Sales
		SUM(s.ITEM_CHRG_AMT) AS Gross_Sales
		-- FROM `wmt-edw-prod.WW_GEC_RPT_VM.ACCT_FIN_SHPD_SALES_MP` s
		FROM `wmt-edw-prod.US_FIN_ECOMM_DL_RPT_VM.ACCT_FIN_SHPD_SALES_MP` s --CHANGE
		WHERE s.EVENT_DT BETWEEN DATE '2024-12-28' AND DATE '2025-04-04'  --UPDATE DATE
	    -- AND s.tenant_org_id IN (0, 4571)                                     --Walmart.com 
			AND s.op_cmpny_cd = "WMT.COM" --CHANGE
	    AND s.svc_id IN (10)                                           --MP
		-- AND s.vertical_id = 0											--Remove OGP
		AND s.VERTICAL_NM in ("0","GM","4")--CHANGE
		GROUP BY 1,2,3
		) s2
	FULL  JOIN
		(SELECT rf.catlg_item_id, rf.event_dt, rf.shpg_node_org_cd,
		--SUM(CAST(rf.REFUNDED_QTY AS DEC(18,2))) AS Total_Units_Rfnd,
        SUM(rf.REFUNDED_QTY) AS Total_Units_Rfnd,		
		--SUM(CAST(rf.ITEM_RFND_CHRG_AMT AS DEC(18,2))) AS Total_Rfnd
		SUM(rf.ITEM_RFND_CHRG_AMT) AS Total_Rfnd
		-- FROM `wmt-edw-prod.WW_GEC_RPT_VM.ACCT_FIN_RFND_SALES_MP` rf
		FROM `wmt-edw-prod.US_FIN_ECOMM_DL_RPT_VM.ACCT_FIN_RFND_SALES_MP` rf --CHANGE
		WHERE rf.EVENT_DT   BETWEEN DATE '2024-12-28' AND DATE'2025-04-04'    --UPDATE DATE
	    -- AND rf.tenant_org_id IN (0, 4571)                                     --Walmart.com 
			AND rf.op_cmpny_cd = "WMT.COM" --CHANGE
	    AND rf.svc_id IN (10)                                           --MP
		-- AND rf.vertical_id = 0											--Remove OGP
		AND rf.VERTICAL_NM in ("0","GM","4") --CHANGE
		GROUP BY 1,2,3
		) rf2
	ON s2.catlg_item_id = rf2.catlg_item_id AND s2.event_dt = rf2.event_dt AND s2.shpg_node_org_cd = rf2.shpg_node_org_cd
	GROUP BY 1,2,3
	) mp
/*
	JOIN     (
			   SELECT         t.catlg_item_id
			   FROM           `wmt-edw-prod.WW_GEC_VM.PROD_CLASS_TYPE_TRK` t
			   WHERE         t.actv_ind = 1
			   AND          t.prod_class_type_id NOT IN (30,47,56)  -- GC Join to exclude
			   ) t
    ON  mp.catlg_item_id = t.catlg_item_id                      

	JOIN     (
			   SELECT         prh.rpt_hrchy_id,
						prh.catlg_item_id
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD_RPT_HRCHY` prh
			   WHERE         prh.curr_ind = 1                       --Item RHID
			   AND prh.catlg_item_id<> -999
			   ) prh
	ON  mp.catlg_item_id = prh.catlg_item_id    
           
	JOIN     (
			   SELECT          rh.rpt_hrchy_id,rh.div_id, rh.super_dept_id, rh.dept_id, rh.categ_id, rh.sub_categ_id  
			   FROM         `wmt-edw-prod.WW_GEC_VM.RPT_HRCHY` rh
			   WHERE         rh.curr_ind = 1                        --RH ID Lookup
			   ) rh
    ON  prh.rpt_hrchy_id = rh.rpt_hrchy_id         
        
------------------------------------------------------------------------------------          
    JOIN     (     
				SELECT catlg_item_id, MAX(upc) AS upc, MAX(GTIN) AS GTIN, MAX(WUPC) AS WUPC
				FROM
				(SELECT      pil.catlg_item_id,  
				CASE WHEN    pil.prod_id_type_id  = 2 THEN pil.prod_id_val END AS UPC,
				CASE WHEN    pil.prod_id_type_id  = 3 THEN pil.prod_id_val END AS GTIN,
				CASE WHEN    pil.prod_id_type_id  = 1 THEN pil.prod_id_val END AS WUPC
				FROM         `wmt-edw-prod.WW_GEC_VM.PROD_ID_LKP` pil
				WHERE        pil.prod_id_rank = 1                     --Remove Duplicate Items
				) a 
				GROUP BY catlg_item_id
			   ) pil
    ON  mp.catlg_item_id = pil.catlg_item_id
	
	JOIN     (     
			   SELECT        prod.catlg_item_id, prod.prod_nm, prod.prod_type_id
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD` prod          
			   ) prod
    ON  mp.catlg_item_id = prod.catlg_item_id
	
	JOIN     (     
			   SELECT        ptl.prod_type_id, ptl.prod_type_nm
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD_TYPE_LKP` ptl            
			   ) ptl
    ON  prod.prod_type_id = ptl.prod_type_id
------------------------------------------------------------------------------------   
     
	JOIN 	(                                          
			SELECT org_id, org_nm                                         
			FROM `wmt-edw-prod.WW_GEC_VM.ORG_SHIP_NODE` osn                                          
			WHERE src_org_key NOT IN ('297072','220815')						--Exclude Warranties, subscriptions
			AND TRIM(osn.org_nm) <>'Walmart.com Subscription'                 -- Exclude Shipping Pass Subscription
			)  osn
    ON  mp.shpg_node_org_cd = osn.org_id
	
	JOIN 	(                                          
			SELECT cal.wm_month_beg_dt, cal.wm_month_end_dt, cal.wm_month_num_of_yr, wm_yr_id                                         
			FROM `wmt-edw-prod.WW_GEC_VM.CAL_WM_MONTH` cal                      --GET WM MONTH NUMBER             
			)  cal
    ON  mp.EVENT_DT BETWEEN cal.wm_month_beg_dt AND cal.wm_month_end_dt
	
	JOIN 	(                                          
			SELECT RIGHT(cal_yr.wm_yr_desc,4) AS wm_year_nbr, cal_yr.wm_yr_id                                         
			FROM `wmt-edw-prod.WW_GEC_VM.CAL_WM_YR` cal_yr                     --GET WM Year Desc             
			)  cal_yr
    ON  cal.wm_yr_id = cal_yr.wm_yr_id */


	JOIN     (
			   /*SELECT         t.catlg_item_id
			   FROM           `wmt-edw-prod.WW_GEC_VM.PROD_CLASS_TYPE_TRK` t
			   WHERE         t.actv_ind = 1  --？？？
			   AND          t.prod_class_type_id NOT IN (30,47,56)  -- GC Join to exclude*/
		-- 		 SELECT t.catlg_item_id
		-- 		 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD` t
		-- 		 WHERE DEL_IND = 0 --CHANGE
		-- 		 AND t.prod_class_type_id NOT IN (30,47,56) --CHANGE
		-- 	   ) t
    -- ON  mp.catlg_item_id = t.catlg_item_id       --> since the del ind flag is not correct
	SELECT catlg_item_id FROM (
	SELECT *,RANK() OVER (PARTITION BY catlg_item_id ORDER BY SRC_MODFD_TS DESC )  as recent_log
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD` t
				 WHERE 
				 t.prod_class_type_id NOT IN (30,47,56)
	 ) 
	 WHERE recent_log =1 )t
	ON mp.catlg_item_id = t.catlg_item_id         

	JOIN     (
			   /*SELECT         prh.rpt_hrchy_id,
						prh.catlg_item_id
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD_RPT_HRCHY` prh
			   WHERE         prh.curr_ind = 1                       --Item RHID --？？？
			   AND prh.catlg_item_id<> -999*/
				 SELECT prh.src_rpt_hrchy_id as rpt_hrchy_id, prh.catlg_item_id
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_RPT_HRCHY` prh
				 WHERE prh.catlg_item_id<> -999 --CHANGE
			   ) prh
	ON  mp.catlg_item_id = prh.catlg_item_id    
           
	JOIN     (
			   /*SELECT          rh.rpt_hrchy_id,rh.div_id, rh.super_dept_id, rh.dept_id, rh.categ_id, rh.sub_categ_id  
			   FROM         `wmt-edw-prod.WW_GEC_VM.RPT_HRCHY` rh
			   WHERE         rh.curr_ind = 1                        --RH ID Lookup*/ --？？？
				 SELECT rh.SRC_RPT_HRCHY_ID as rpt_hrchy_id,rh.rpt_hrchy_lvl0_id as div_id, rh.rpt_hrchy_lvl1_id as super_dept_id, rh.rpt_hrchy_lvl2_id as dept_id, rh.rpt_hrchy_lvl3_id as categ_id, rh.rpt_hrchy_lvl4_id as sub_categ_id
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.RPT_HRCHY` rh --CHANGE
			   ) rh
    ON  prh.rpt_hrchy_id = rh.rpt_hrchy_id         
------------------------------------------------------------------------------------          
    JOIN     (     
				/*SELECT catlg_item_id, MAX(upc) AS upc, MAX(GTIN) AS GTIN, MAX(WUPC) AS WUPC
				FROM
				(SELECT      pil.catlg_item_id,  
				CASE WHEN    pil.prod_id_type_id  = 2 THEN pil.prod_id_val END AS UPC,
				CASE WHEN    pil.prod_id_type_id  = 3 THEN pil.prod_id_val END AS GTIN,
				CASE WHEN    pil.prod_id_type_id  = 1 THEN pil.prod_id_val END AS WUPC
				FROM         `wmt-edw-prod.WW_GEC_VM.PROD_ID_LKP` pil
				WHERE        pil.prod_id_rank = 1                     --Remove Duplicate Items
				) a 
				GROUP BY catlg_item_id*/
				SELECT catlg_item_id, MAX(upc) AS upc, MAX(GTIN) AS GTIN, MAX(WUPC) AS WUPC
FROM
(SELECT pil.catlg_item_id,
CASE WHEN pil.PROD_ID_TYPE_NM = 'UPC' THEN pil.prod_id_val END AS UPC,
CASE WHEN pil.PROD_ID_TYPE_NM = 'GTIN' THEN pil.prod_id_val END AS GTIN,
CASE WHEN pil.PROD_ID_TYPE_NM = 'WUPC' THEN pil.prod_id_val END AS WUPC
FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_ALT_ID` pil
WHERE pil.PROD_ID_RANK_NBR = 1 --Remove Duplicate Items
) a
GROUP BY catlg_item_id
			   ) pil
    ON  mp.catlg_item_id = pil.catlg_item_id
	JOIN     (     
			   /*SELECT        prod.catlg_item_id, prod.prod_nm, prod.prod_type_id
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD` prod    */
				 SELECT prod.catlg_item_id, prod.prod_nm, prod.prod_type_nm
				 FROM `wmt-edw-prod.WW_PRODUCT_DL_VM.PROD` prod        
			   ) prod
    ON  mp.catlg_item_id = prod.catlg_item_id
	
	/*JOIN     (     
			   SELECT        ptl.prod_type_id, ptl.prod_type_nm
			   FROM         `wmt-edw-prod.WW_GEC_VM.PROD_TYPE_LKP` ptl              
			   ) ptl
    ON  prod.prod_type_id = ptl.prod_type_id*/
------------------------------------------------------------------------------------   
     
	JOIN 	(                                          
			/*SELECT org_id, org_nm                                         
			FROM `wmt-edw-prod.WW_GEC_VM.ORG_SHIP_NODE` osn                                          
			WHERE src_org_key NOT IN ('297072','220815')						--Exclude Warranties, subscriptions
			AND TRIM(osn.org_nm) <>'Walmart.com Subscription'                 -- Exclude Shipping Pass Subscription*/
			SELECT distinct org_cd ---ISSUE!!!!!!!!!!!
			FROM `wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.ORG_SHIP_NODE` osn --CHANGE
			WHERE src_org_key NOT IN ('297072','220815')
			AND TRIM(osn.org_nm) <>'Walmart.com Subscription'
			)  osn
    ON  mp.shpg_node_org_cd = osn.org_cd
	JOIN 	(                                          
			/*SELECT cal.wm_month_beg_dt, cal.wm_month_end_dt, cal.wm_month_num_of_yr, wm_yr_id                                         
			FROM `wmt-edw-prod.WW_GEC_VM.CAL_WM_MONTH` cal                      --GET WM MONTH NUMBER       */
			SELECT cal.wm_mth_beg_dt, cal.wm_mth_end_dt, cal.wm_mth_nbr_of_yr_nbr as wm_month_num_of_yr, wm_yr_id
			FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.CAL_WM_MONTH` cal --CHANGE
			)  cal
    ON  mp.EVENT_DT BETWEEN cal.wm_mth_beg_dt AND cal.wm_mth_end_dt
		JOIN 	(                                          
			/* SELECT RIGHT(cal_yr.wm_yr_desc,4) AS wm_year_nbr, cal_yr.wm_yr_id                                         
			FROM `wmt-edw-prod.WW_GEC_VM.CAL_WM_YR` cal_yr                     --GET WM Year Desc    */
			SELECT RIGHT(cal_yr.wm_yr_desc,4) AS wm_year_nbr, cal_yr.wm_yr_id
			FROM `wmt-edw-prod.WW_CORE_DIM_DL_VM.CAL_WM_YR` cal_yr --CHANGE    
			)  cal_yr
    ON  cal.wm_yr_id = cal_yr.wm_yr_id	 


GROUP BY mp.catlg_item_id, rh.div_id, rh.super_dept_id, rh.dept_id, rh.categ_id, rh.sub_categ_id, pil.UPC, pil.GTIN, pil.WUPC ,prod.prod_nm, 
			prod.prod_type_nm, cal_yr.wm_year_nbr, cal.wm_month_num_of_yr) --WITH DATA
--PRIMARY INDEX (catlg_item_id, div_id, super_dept_id, dept_id, categ_id, sub_categ_id, wm_year_nbr, wm_month_num_of_yr)
;


-----Update Create table name AND 4P and MP table names to match previous queries

CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_dotcom_5p_FY26Mar` --UPDATE
AS
	(SELECT
	CASE WHEN p4.catlg_item_id IS NOT NULL THEN p4.catlg_item_id ELSE mp.catlg_item_id END AS catlg_item_id,
	CASE WHEN p4.DIV_ID IS NOT NULL THEN p4.DIV_ID ELSE mp.DIV_ID END AS DIV_ID,
	CASE WHEN p4.SUPER_DEPT_ID IS NOT NULL THEN p4.SUPER_DEPT_ID ELSE mp.SUPER_DEPT_ID END AS SUPER_DEPT_ID,
	CASE WHEN p4.DEPT_ID IS NOT NULL THEN p4.DEPT_ID ELSE mp.DEPT_ID END AS DEPT_ID,
	CASE WHEN p4.CATEG_ID IS NOT NULL THEN p4.CATEG_ID ELSE mp.CATEG_ID END AS CATEG_ID,
	CASE WHEN p4.SUB_CATEG_ID IS NOT NULL THEN p4.SUB_CATEG_ID ELSE mp.SUB_CATEG_ID END AS SUB_CATEG_ID,
	CASE WHEN p4.upc IS NOT NULL THEN p4.upc ELSE mp.upc END AS upc,
	CASE WHEN p4.GTIN IS NOT NULL THEN p4.GTIN ELSE mp.GTIN END AS GTIN,
	CASE WHEN p4.WUPC IS NOT NULL THEN p4.WUPC ELSE mp.WUPC END AS WUPC,
	CASE WHEN p4.PROD_NM IS NOT NULL THEN p4.PROD_NM ELSE mp.PROD_NM END AS PROD_NM,
	CASE WHEN p4.PROD_TYPE_NM IS NOT NULL THEN p4.PROD_TYPE_NM ELSE mp.PROD_TYPE_NM END AS PROD_TYPE_NM,
	CASE WHEN p4.wm_year_nbr IS NOT NULL THEN p4.wm_year_nbr ELSE mp.wm_year_nbr END AS wm_year_nbr,	
	CASE WHEN p4.WM_MONTH_NUM_OF_YR IS NOT NULL THEN p4.WM_MONTH_NUM_OF_YR ELSE mp.WM_MONTH_NUM_OF_YR END AS WM_MONTH_NUM_OF_YR,
	SUM(S2H) AS S2H_Sales_Net, SUM(S2S) AS S2S_Sales_Net, SUM(PUT) AS PUT_Sales_Net, SUM(SFS) AS SFS_Sales_Net, SUM(P4SalesNet) AS P4_Sales_Net, SUM(S2H_Units) AS S2H_Units_Net, SUM(S2S_Units) AS S2S_Units_Net, SUM(PUT_Units) AS PUT_Units_Net, SUM(SFS_Units) AS SFS_Units_Net, SUM(P4_Units) AS P4_Units_Net, SUM(MPSalesNet) AS MP_Sales_Net, SUM(MP_Units) AS MP_Units_Net
	
	FROM
		(
		SELECT	catlg_item_id, DIV_ID, SUPER_DEPT_ID, DEPT_ID, CATEG_ID, SUB_CATEG_ID, upc, GTIN, WUPC, PROD_NM, PROD_TYPE_NM, wm_year_nbr, WM_MONTH_NUM_OF_YR, SUM(S2H_Sales_Net) AS S2H, SUM(S2S_Sales_Net) AS S2S, SUM(PUT_Sales_Net) AS PUT, SUM(SFS_Sales_Net) AS SFS, SUM(P4_Sales_Net) AS P4SalesNet, SUM(S2H_Units_Net) AS S2H_Units, SUM(S2S_Units_Net) S2S_Units, SUM(PUT_Units_Net) PUT_Units, SUM(SFS_Units_Net) SFS_Units, SUM(P4_Units_Net) AS P4_Units
		FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_dotcom_4p_FY26Mar` p4 --UPDATE
		GROUP BY catlg_item_id, DIV_ID, SUPER_DEPT_ID, DEPT_ID, CATEG_ID, SUB_CATEG_ID, upc, GTIN, WUPC, PROD_NM, PROD_TYPE_NM, wm_year_nbr, WM_MONTH_NUM_OF_YR
		) p4

		FULL JOIN

		(
		SELECT	catlg_item_id, DIV_ID, SUPER_DEPT_ID, DEPT_ID, CATEG_ID, SUB_CATEG_ID, upc, GTIN, WUPC, PROD_NM, PROD_TYPE_NM, wm_year_nbr, WM_MONTH_NUM_OF_YR, SUM(MP_Sales_Net) AS MPSalesNet, SUM(MP_Units_Net) AS MP_Units
		FROM `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_dotcom_mp_FY26Mar` mp --UPDATE
		GROUP BY catlg_item_id, DIV_ID, SUPER_DEPT_ID, DEPT_ID, CATEG_ID, SUB_CATEG_ID, upc, GTIN, WUPC, PROD_NM, PROD_TYPE_NM, wm_year_nbr, WM_MONTH_NUM_OF_YR
		) mp

		ON p4.catlg_item_id = mp.catlg_item_id AND p4.DIV_ID = mp.DIV_ID AND p4.SUPER_DEPT_ID = mp.SUPER_DEPT_ID AND p4.DEPT_ID = mp.DEPT_ID AND p4.CATEG_ID = mp.categ_id AND p4.SUB_CATEG_ID = mp.SUB_CATEG_ID AND p4.wm_year_nbr = mp.wm_year_nbr AND p4.wm_month_num_of_yr = mp.wm_month_num_of_yr

	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13); --WITH DATA
