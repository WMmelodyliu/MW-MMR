-- update month in table name
-- update date range (recent 4 month)


-- Online Sales Pre --
create or replace table wmt-mint-mmr-mw-prod.mw_numerator_dev.online_sales_FY26Feb_pre as -- update name
(

SELECT 
		CAL_KEY,  
		ACCTG_DEPT_NBR,
		RPT_LVL_4_ID,
		CATLG_ITEM_ID,
		RPT_DT,
		CHNL,   
		SUM(GMV_ACTL) AS GMV_ACTL,
--		SUM(Units) AS ACTL_UNITS_SOLD,
--		SUM(NET_SALES_ACTL) NET_SALES_ACTL,
		ind,
		ind2,
		sno
FROM (
SELECT
CAL_KEY,  
A.ACCTG_DEPT_NBR,
RPT_LVL_4_ID,
CATLG_ITEM_ID,
CALENDAR_DATE AS RPT_DT,
CHNL,   
TY_GMV AS GMV_ACTL,
--0 AS MP_Commissions,
--0 AS Units,
--0 AS IMU,
--0 AS CP,
--0 AS GMV_Calc_Amt,
--TY_NETSALES AS NET_SALES_ACTL,
ind,
ind2,
sno
FROM 
(
SELECT CAL_KEY, 
	   CALENDAR_DATE,
	   ACCTG_DEPT_NBR,
	   RPT_LVL_4_ID,
	   CATLG_ITEM_ID,
       CHNL,
       SUM(REV_GMV) AS TY_GMV,
--	   SUM(REV_NS) AS TY_NETSALES,
	   ind,
	   ind2,
	   sno
FROM (
SELECT
	CAST(replace(CAST(CALENDAR_DATE as STRING), '-', '') AS INT64) AS CAL_KEY,
    CALENDAR_DATE,
	ACCTG_DEPT_NBR,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	CASE WHEN CHANNEL IN ('DFS','SFS' ) THEN 'SFS' 
         ELSE CHANNEL END AS CHNL,
	SUM(REV_GMV) AS REV_GMV,
--    SUM(REV_NS) AS REV_NS, 
	ind,
	ind2,
	sno
FROM 
(
SELECT
	CALENDAR_DATE,
	ACCTG_DEPT_NBR,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	CHANNEL,
	SUM(REV_GMV) AS REV_GMV,
	--SUM(REV_NS) AS REV_NS,
	ind,
	ind2,
	sno
FROM(
SELECT 
	CALENDAR_DATE,
	ACCTG_DEPT_NBR,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	CHANNEL,
	SUM(Sales) AS REV_GMV,
	--SUM(Sales) as REV_NS,
	ind,
	ind2,
	sno
FROM(
Select 
	CALENDAR_DATE,
	ACCTG_DEPT_NBR,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	'S2H' CHANNEL,
	SUM(IFNULL(Virtual_value,0) +  IFNULL(Shipping_Revenue,0)+IFNULL(Cash_CSA,0)+IFNULL(Credit_Charge_back,0)+IFNULL(Liquidation_Sales,0)+ifnull(B2B,0)+ifnull(Discount_Amt,0)+
	IFNULL(Core_Merch_Sales,0)+ifnull(FC_Exits,0)+ifnull(Associate_Purchase_Discount,0)+ifnull(Contacts,0)+IFNULL(PetRx,0)+IFNULL(Services, 0)+
	IFNULL(Core_Merch_Sales5,0)+ifnull(Store_Refunds,0)) AS Sales, ind, ind2,sno
FROM 
(
--*************************************************************************************************
--(28$)
SELECT 
A1.calendar_date,
extract(MONTH from A1.calendar_date) AS mnth1,
extract(YEAR from A1.calendar_date) AS yr1,
acctg_dept_nbr,
RPT_LVL_4_ID,
CATLG_ITEM_ID,
CASE WHEN SAP_act_virtual = 0 THEN tot_virtual*(deprt_tot_virtual/tot_virtual) ELSE SAP_Act_virtual*(deprt_tot_virtual/tot_virtual)*-1 end AS Virtual_value, 
CASE WHEN SAP_Act_Shipping_Revenue = 0 THEN tot_Shipping_Revenue*(deprt_tot_Shipping_Revenue/tot_Shipping_Revenue) ELSE SAP_Act_Shipping_Revenue*(deprt_tot_Shipping_Revenue/tot_Shipping_Revenue)*-1 end AS Shipping_Revenue,
CASE WHEN SAP_Act_Cash_CSA = 0 THEN tot_Cash_CSA*(deprt_tot_Cash_CSA/tot_Cash_CSA) ELSE SAP_Act_Cash_CSA*(deprt_tot_Cash_CSA/tot_Cash_CSA)*-1 end AS Cash_CSA,
SAP_Act_Credit_Charge_back*(deprt_tot_Credit_Charge_back/tot_Credit_Charge_back) *-1 AS Credit_Charge_back,
SAP_Act_Liquidation_Sales*(deprt_tot_Liquidation_Sales/tot_Liquidation_Sales) *-1 AS Liquidation_Sales,
SAP_Act_B2B*(deprt_tot_Liquidation_Sales/tot_Liquidation_Sales) *-1 AS B2B,
tot_Discount_Amt*(deprt_tot_Discount_Amt/tot_Discount_Amt)  AS Discount_Amt,
SAP_Act_FC_Exits*(deprt_tot_Liquidation_Sales/tot_Liquidation_Sales) *-1 AS Core_Merch_Sales,
SAP_Act_FC_Exits*(deprt_tot_Liquidation_Sales/tot_Liquidation_Sales) *-1 AS FC_Exits,
tot_Associate_Purchase_Discount*(deprt_tot_Associate_Purchase_Discount/tot_Associate_Purchase_Discount)  AS Associate_Purchase_Discount,
NULL  AS Contacts,
NULL PetRx,
NULL Services, 
NULL Core_Merch_Sales5,
NULL Store_Refunds,
'online' as ind, 
'core eComm' as ind2, 
28 as sno
FROM 
(
 SELECT Event_dt AS calendar_date,
      cast(TRIM(LEFT( TSKU.rpt_lvl_1_nm,2)) AS int64)  acctg_dept_nbr,
	  TSKU.RPT_LVL_4_ID,
	  TSKU.CATLG_ITEM_ID,
      SUM(RFND_RTL_SALES)  deprt_tot_virtual,
      SUM(SUM(RFND_RTL_SALES)) over ( partition by Event_dt ) AS tot_virtual,

      SUM(GROSS_SHPG_REV_AMT)  deprt_tot_Shipping_Revenue,
      SUM(SUM(GROSS_SHPG_REV_AMT)) over ( partition by Event_dt ) AS tot_Shipping_Revenue,

      SUM(CSA_MERCH_AMT)  deprt_tot_Cash_CSA,
      SUM(SUM(CSA_MERCH_AMT)) over ( partition by Event_dt ) AS tot_Cash_CSA,

      SUM(GROSS_SALES_REV_AMT)  deprt_tot_Credit_Charge_back,
      SUM(SUM(GROSS_SALES_REV_AMT)) over ( partition by Event_dt ) AS tot_Credit_Charge_back,

      SUM(GROSS_SALES_REV_AMT)  deprt_tot_Liquidation_Sales,
      SUM(SUM(GROSS_SALES_REV_AMT)) over ( partition by Event_dt ) AS tot_Liquidation_Sales,
      
      SUM(PROMO_AMT)  deprt_tot_Discount_Amt,
      SUM(SUM(PROMO_AMT)) over ( partition by Event_dt ) AS tot_Discount_Amt,

      SUM(ASSOC_DISC_AMT)  deprt_tot_Associate_Purchase_Discount,
      SUM(SUM(ASSOC_DISC_AMT)) over ( partition by Event_dt ) AS tot_Associate_Purchase_Discount,

  FROM  wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
  `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU
  WHERE  EVENT_DT between date('2024-10-26') and date('2025-02-28') -- update date
  AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
  AND svc_id IN (0,8)
  AND rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
  GROUP BY 1,2,3,4
) A1
  LEFT OUTER JOIN
(
SELECT 
postg_dt AS calendar_date,
SUM(case when (fin_cmpny_cd = 'A148' and PROFIT_CNTR_NBR IN ('US08819G','US09462G') AND rpt_acct_nbr =4101090) then  trans_amt else 0 end ) AS SAP_Act_virtual,
SUM(case when (fin_cmpny_cd = 'A148' and rpt_acct_nbr =4104001) then  trans_amt else 0 end ) AS SAP_Act_Shipping_Revenue,
SUM(case when (fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR='US08819G' AND rpt_acct_nbr =4101010) then  local_crncy_amt else 0 end ) AS SAP_Act_Cash_CSA,
SUM(case when (fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR='US10669G' AND rpt_acct_nbr =4101091) then  local_crncy_amt else 0 end ) AS SAP_Act_Credit_Charge_back,
SUM(case when (fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR='US09462G' AND rpt_acct_nbr =4101010) then  local_crncy_amt else 0 end ) AS SAP_Act_Liquidation_Sales,
SUM(case when (fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR='US03467G' AND rpt_acct_nbr =4101010) then  local_crncy_amt else 0 end ) AS SAP_Act_B2B,
SUM(case when (fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR='US02701G1' AND rpt_acct_nbr =4101010) then  local_crncy_amt else 0 end ) AS SAP_Act_FC_Exits,
SUM(case when fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR IN ('US10504G')  and rpt_acct_nbr IN (4101010) then  local_crncy_amt*-1 else 0 end ) AS SAP_Act_Contacts,
SUM(case when fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR IN ('US10504G')  and rpt_acct_nbr IN (4101030) then  local_crncy_amt*-1 else 0 end ) AS SAP_Act_PetRx,
SUM(case when fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR IN ('US09898G','US10814G') and rpt_acct_nbr IN (4101030,4102003,4103009,4103021,4103022,4103024,4103030,4103031,4103032,4103036,4101010,4101030,4102001,4103009,4103021,4103022,4103024,4103029,4103030,4103031,4103032,4103036)  then  local_crncy_amt*-1 else 0 end ) AS SAP_Act_Services,
SUM(case when fin_cmpny_cd IN ('A148','A217') and PROFIT_CNTR_NBR = 'US09738G1'   and rpt_acct_nbr IN (4101010) then  local_crncy_amt*-1 else 0 end ) AS SAP_Act_Core_Merch_Sales5
FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY
WHERE  postg_dt  between date('2024-10-26') and date('2025-02-28') -- update date
GROUP BY 1
) A2
ON A1.calendar_date =A2.calendar_date
--(28$)
--*************************************************************************************************

UNION ALL 
--*************************************************************************************************
--(27$)
SELECT 
	EVENT_DT AS calendar_date,
	extract(MONTH from Event_dt) AS mnth1,
	extract(YEAR from  Event_dt) AS yr1,
	cast(TRIM(LEFT( TSKU.rpt_lvl_1_nm,2)) AS int64)  acctg_dept_nbr,
	TSKU.RPT_LVL_4_ID,
	TSKU.CATLG_ITEM_ID,
	NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, NULL Discount_Amt,
	SUM(GROSS_SALES_REV_AMT) Core_Merch_Sales,
	NULL FC_Exits, NULL Associate_Purchase_Discount,
	NULL Contacts,
	NULL PetRx,
	NULL Services, 
	NULL Core_Merch_Sales5,
	NULL Store_Refunds,
	'online' as ind, 
	'core eComm' as ind2, 
	27 as sno
FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE AS FACT
LEFT JOIN wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU TSKU
ON FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
WHERE FACT.SVC_ID IN (0,8)
AND EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
AND rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
AND IFNULL(OWN_PRMRY_VEND_ID,'0') not IN ('285986')
GROUP BY 1,2,3,4,5,6   
--(27$)
 --*************************************************************************************************

UNION ALL

--*************************************************************************************************
--(26$)--Store refunds
SELECT 
calendar_date,
mnth1,
yr1,
acctg_dept_nbr,
RPT_LVL_4_ID,
CATLG_ITEM_ID,
NULL Virtual_value,  NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, NULL Discount_Amt,
NULL Core_Merch_Sales, NULL FC_Exits, NULL Associate_Purchase_Discount, NULL Contacts,NULL PetRx, NULL Services, NULL Core_Merch_Sales5,
Store_Refunds *-1 as Store_Refunds,
'online' as ind, 
'core eComm' as ind2,
26 as sno
FROM(
SELECT 
A1.calendar_date,
extract(MONTH from A1.calendar_date) AS mnth1,
extract(YEAR from A1.calendar_date) AS yr1,
acctg_dept_nbr,
RPT_LVL_4_ID,
CATLG_ITEM_ID,
NULL Virtual_value,  NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, NULL Discount_Amt,
NULL Core_Merch_Sales, NULL FC_Exits, NULL Associate_Purchase_Discount, NULL Contacts,NULL PetRx, NULL Services, NULL Core_Merch_Sales5,
CASE WHEN SAP_Act_Store_Refunds is NULL THEN deprt_tot_Store_Refunds ELSE (deprt_tot_Store_Refunds/tot_Store_Refunds)* SAP_Act_Store_Refunds   END AS Store_Refunds
FROM 
(
SELECT
      ROL.RTN_CRE_DT AS calendar_date,
      EXTRACT(MONTH from ROL.RTN_CRE_DT) AS mnth1,
      extract(YEAR from ROL.RTN_CRE_DT) AS yr1,
      CAST(TRIM(LEFT( RP.RPT_HRCHY_LVL1_DESC,2)) AS INT)  acctg_dept_nbr,
	  RP.RPT_HRCHY_LVL4_ID as RPT_LVL_4_ID,
	  ROL.CATLG_ITEM_ID,
      SUM(ROL.UNIT_PRICE * ROL.EXPC_QTY)  deprt_tot_Store_Refunds,  
      SUM(SUM(ROL.UNIT_PRICE * ROL.EXPC_QTY)) over ( partition by extract(MONTH from ROL.RTN_CRE_DT), extract(YEAR from ROL.RTN_CRE_DT) ) AS tot_Store_Refunds
FROM wmt-edw-prod.WW_RTN_DL_VM.RTN_360_DTL ROL 
LEFT JOIN wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_SHPD_SALES_REV FSSR               
ON ROL.SALES_ORDER_NUM  = FSSR.SALES_ORDER_NUM 
AND ROL.SALES_ORDER_LINE_NUM  = FSSR.SALES_ORDER_LINE_NUM 
AND ROL.SHPMNT_NUM   = FSSR.SHPMNT_NUM 
-- AND FSSR.REV_TYPE_ID = 1 WW_GEC_VM
AND FSSR.REV_TYPE_CD = "SHIP_SALES"
AND FSSR.EVENT_DT >=  DATE_SUB('2021-04-01', INTERVAL 5 YEAR) --modify dates here
LEFT JOIN wmt-edw-prod.WW_PRODUCT_DL_VM.PROD PRD	 ON  ROL.CATLG_ITEM_ID =  PRD.CATLG_ITEM_ID	
LEFT JOIN wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_RPT_HRCHY PRH 	ON PRH.CATLG_ITEM_ID = PRD.CATLG_ITEM_ID 	AND PRD.CATLG_ITEM_ID = PRH.CATLG_ITEM_ID 
LEFT JOIN  wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_ALT_ID PIL	ON PRD.CATLG_ITEM_ID = PIL.CATLG_ITEM_ID	 AND  PIL.CATLG_ITEM_ID=PRD.CATLG_ITEM_ID  	
AND PIL.prod_id_rank_nbr = 1 
AND PIL.PROD_ID_TYPE_NM = 'WUPC'	           	      
LEFT JOIN  wmt-edw-prod.WW_PRODUCT_DL_VM.RPT_HRCHY RP	      
ON RP.SRC_RPT_HRCHY_ID = PRH.SRC_RPT_HRCHY_ID	
WHERE ROL.RTN_CRE_DT between date('2024-10-26') and date('2025-02-28') -- update date---->date to be changed
AND ROL.TENANT_ORG_ID = 4571
-- AND FSSR.TENANT_ORG_ID = 4571 WW_GEC_VM
AND FSSR.OP_CMPNY_CD = "OP_CMPNY_CD"
-- AND ROL.RTN_TYPE_ID = 2  WW_GEC_VM
AND ROL.RTN_TYPE_DESC = "STORE"
AND COALESCE(FSSR.SVC_ID,0) IN (0,8) 
AND RP.RPT_HRCHY_LVL0_DESC NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
AND RP.RPT_HRCHY_LVL0_DESC is not NULL
GROUP BY 1, 2,3,4,5,6
) A1
LEFT OUTER JOIN
(
SELECT 
extract(MONTH from postg_dt) AS mnth1,
extract(YEAR from postg_dt) AS yr1,
SUM(trans_amt) AS SAP_Act_Store_Refunds
FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY,
( SELECT a2.calendar_date
FROM (SELECT 1 AS ind, cal_epoch_mo_cnt FROM `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a WHERE calendar_date = CURRENT_DATE()-33) a1,
`wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a2 WHERE   a2.cal_epoch_mo_cnt<=a1.cal_epoch_mo_cnt 
AND a2.calendar_date between date('2024-10-26') and date('2025-02-28') -- update date 
GROUP BY 1) a2 
WHERE fin_cmpny_cd = 'A148'
AND rpt_acct_nbr =4101090
AND postg_dt = a2.calendar_Date
AND PROFIT_CNTR_NBR='US02677G' 
AND postg_dt  between date('2024-10-26') and date('2025-02-28') -- update date
GROUP BY 1,2
) A2
ON A1.mnth1 = A2.mnth1 and A1.yr1 = A2.yr1
)

--(26$)
--*************************************************************************************************	

   
UNION ALL
--*************************************************************************************************	
--(25$) -- Core_Merch_Sales5 
SELECT 
postg_dt calendar_date,
extract(MONTH from postg_dt) AS mnth1,
extract(YEAR from postg_dt) AS yr1,
5 AS acctg_dept_nbr,
0 as dept_subcatg_nbr,
0 as mds_fam_id,
NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, NULL Discount_Amt,NULL Core_Merch_Sales,
NULL FC_Exits, NULL Associate_Purchase_Discount, NULL Contacts,NULL PetRx,NULL Services,
SUM (local_crncy_amt*-1) AS Core_Merch_Sales5,
NULL AS Store_Refunds, 
'no item' as ind, 
'core eComm' as ind2 , 
25 as sno  
FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY
WHERE fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
AND PROFIT_CNTR_NBR = 'US09738G1'                              --For CSA
AND rpt_acct_nbr IN (4101010)                                 
AND postg_dt between date('2024-10-26') and date('2025-02-28') -- update date                 
group by 1,2,3  
--(25$) 
--*************************************************************************************************	

UNION ALL

/*Core Services & Third Party Website*/
--*************************************************************************************************	
--(24$)--Services
SELECT
	CALENDAR_DATE,	MNTH1, YR1,		
	ACCTG_DEPT_NBR,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, 
	NULL Discount_Amt,	NULL Core_Merch_Sales,	NULL FC_Exits, NULL Associate_Purchase_Discount, 
	SUM(Contacts) AS Contacts, 
	SUM(PetRx) AS PetRx,
	SUM(Services) AS Services,
	NULL Core_Merch_Sales5,
	NULL AS Store_Refunds,
    ind, 
	ind2, 
    sno
FROM (

--*************************************************************************************************	
--(23$)
SELECT
	CALENDAR_DATE, MNTH1, YR1,		
	ACCTG_DEPT_NBR,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	'S2H' AS CHANNEL,	
	NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, 
	NULL Discount_Amt,	NULL Core_Merch_Sales,	NULL FC_Exits, NULL Associate_Purchase_Discount, 
	NULL AS Contacts, NULL AS PetRx,
	SUM(Services) AS Services,
	NULL Core_Merch_Sales5,
	NULL AS Store_Refunds, 
	ind, 
	ind2, 
	sno
FROM (

--*************************************************************************************************
--(22$)
SELECT
	CALENDAR_DATE,	MTH1 AS MNTH1,	YR1 AS YR1,		
	ACCTG_DEPT_NBR,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	'S2H' AS CHANNEL,	
	NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, 
	NULL Discount_Amt,	NULL Core_Merch_Sales,	NULL FC_Exits, NULL Associate_Purchase_Discount, 
	NULL AS Contacts, NULL AS PetRx,
	Value AS Services,
	NULL Core_Merch_Sales5,
	NULL AS Store_Refunds, 
	'online' as ind, 
	'core eComm' as ind2, 
	sno
FROM
(

--*************************************************************************************************	
--(21$)	

SELECT
	t1.calendar_date,	t1.mth1,	t1.yr1,	t1.acctg_dept_nbr, RPT_LVL_4_ID, T1.CATLG_ITEM_ID,	t1.sls,	t1.ttl_sls,	t2.SAP_Act,
	CASE WHEN SAP_Act is NULL THEN sls ELSE sls/ttl_sls*sap_act END AS Value, 
	'online' as ind, 
	'core eComm' as ind2,
	21 as sno
FROM
(
SELECT
	calendar_date,	a1.mth1,	a1.yr1,	acctg_dept_nbr,RPT_LVL_4_ID,CATLG_ITEM_ID, SUM(sls) AS sls,
	SUM(ttl_sls) AS ttl_sls
FROM
(
SELECT
	calendar_date,
	extract(MONTH from calendar_date) AS mth1,
	extract(YEAR  from calendar_date) AS yr1,
	acctg_dept_nbr,
	RPT_LVL_4_ID,
	CATLG_ITEM_ID,
	SUM(Commissions) AS sls
FROM
(
SELECT
	event_dt AS calendar_date,
	CASE WHEN rpt_lvl_3_nm='COMPUTING PP ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='TELEVISIONS PP ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='WIRELESS PP ECOMM' THEN 87 ELSE 
	CASE WHEN rpt_lvl_3_nm='VG HARDWARE AND ACCESSORIES PP ECOMM' THEN 5 ELSE 
	CASE WHEN rpt_lvl_3_nm='LARGE APPLIANCES AND STORAGE PP ECOMM' THEN 74 ELSE 
	CASE WHEN rpt_lvl_3_nm='RIDE ONS PP ECOMM' THEN 7 ELSE 
	CASE WHEN rpt_lvl_3_nm='TAV ACCESSORIES PP ECOMM' THEN 7 ELSE 
	CASE WHEN rpt_lvl_3_nm='FURNITURE PP ECOMM' THEN 71 ELSE 
	CASE WHEN rpt_lvl_3_nm='SMALL APPLIANCES AND MICROWAVES ECOMM' THEN 14 ELSE 
	CASE WHEN rpt_lvl_3_nm='FITNESS PP ECOMM' THEN 9 ELSE 
	CASE WHEN rpt_lvl_3_nm='TABLETS PP ECOMM' THEN 5 ELSE 
	CASE WHEN rpt_lvl_3_nm='CAMERAS PP ECOMM' THEN 6 ELSE 
	CASE WHEN rpt_lvl_3_nm='COOLING AND HEATING PP ECOMM' THEN 16 ELSE 
	CASE WHEN rpt_lvl_3_nm='OUTDOOR POWER EQUIPMENT PP ECOMM' THEN 16 ELSE 
	CASE WHEN rpt_lvl_3_nm='TRANSFER' THEN 86 ELSE 
	CASE WHEN rpt_lvl_3_nm='FURNITURE HS ECOMM' THEN 71 ELSE 
	CASE WHEN rpt_lvl_3_nm='NETWORKING AND CONNECTIVITY PP ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='SHAVE AND GROOMING PP ECOMM' THEN 2 ELSE 
	CASE WHEN rpt_lvl_3_nm='AUTO BATTERIES AND ELECTRICAL PP ECOMM' THEN 10 ELSE 
	CASE WHEN rpt_lvl_3_nm='AUDIO PP ECOMM' THEN 5 ELSE 
	CASE WHEN rpt_lvl_3_nm='TELEVISIONS HS ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='BABY LARGE GEAR PP ECOMM' THEN 79 ELSE 
	CASE WHEN rpt_lvl_3_nm='FINE JEWELRY PP ECOMM' THEN 32 ELSE 
	CASE WHEN rpt_lvl_3_nm='FITNESS HS ECOMM' THEN 9 ELSE 
	CASE WHEN rpt_lvl_3_nm='COMPUTING HS ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='BIKES HS ECOMM' THEN 9 ELSE 
	CASE WHEN rpt_lvl_3_nm='WATCHES WPP ECOMM' THEN 32 ELSE 
	CASE WHEN rpt_lvl_3_nm='PRINTING PP ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='OUTDOOR POWER EQUIPMENT HS ECOMM' THEN 11 ELSE 
	CASE WHEN rpt_lvl_3_nm='SUMMER SEASONAL PP ECOMM' THEN 7 ELSE 
	CASE WHEN rpt_lvl_3_nm='INDOOR AND OUTDOOR GAMES HS ECOMM' THEN 7 ELSE 
	CASE WHEN rpt_lvl_3_nm='HOUSEHOLD CHEMICALS GROUP HS ECOMM' THEN 13 ELSE 
	CASE WHEN rpt_lvl_3_nm='NETWORKING AND CONNECTIVITY HS ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='OUTDOOR PLAY HS ECOMM' THEN 9 ELSE 
	CASE WHEN rpt_lvl_3_nm='BABY AND TODDLER FURNITURE HS ECOMM' THEN 79 ELSE 
	CASE WHEN rpt_lvl_3_nm='TABLETS HS ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='LIGHTING HS ECOMM' THEN 74 ELSE 
	CASE WHEN rpt_lvl_3_nm='HEALTH CARE HS ECOMM' THEN 40 ELSE 
	CASE WHEN rpt_lvl_3_nm='PRINTING HS ECOMM' THEN 72 ELSE 
	CASE WHEN rpt_lvl_3_nm='LIGHTING AND FASTENERS HS ECOMM' THEN 11 ELSE 
	CASE WHEN rpt_lvl_3_nm='BATH AND KITCHEN REMODEL ECOMM HS ECOMM' THEN 14 ELSE 
	CASE WHEN rpt_lvl_3_nm='COOLING AND HEATING HS ECOMM' THEN 11 ELSE 
	CASE WHEN rpt_lvl_3_nm='HOME WINDOW HS ECOMM' THEN 11 ELSE 
	CASE WHEN rpt_lvl_3_nm='PATIO FURNITURE HS ECOMM' THEN 11 ELSE 
	CASE WHEN rpt_lvl_3_nm='NURSERY SOFT GOODS AND SAFETY HS ECOMM' THEN 11 ELSE 
	CASE WHEN rpt_lvl_3_nm='SUMMER SEASONAL HS ECOMM' THEN 11 ELSE 
	CASE WHEN rpt_lvl_3_nm='AUDIO HS ECOMM' THEN 5 ELSE 
	CASE WHEN rpt_lvl_3_nm='BABY SMALL GEAR HS ECOMM' THEN 79 ELSE 
	CASE WHEN rpt_lvl_3_nm='BABY LARGE GEAR HS ECOMM' THEN 79 ELSE 
	CASE WHEN rpt_lvl_3_nm='SERVICES MISC L3' THEN 99 ELSE 99
	END END END END END END END END END END END END END 
	END END END END END END END END END END END END END 
	END END END END END END END END END END END END END
	END END END END END END END END END END END AS Acctg_dept_nbr,
	TSKU.RPT_LVL_4_ID,
	FACT.CATLG_ITEM_ID,
	SUM(GROSS_SALES_REV_AMT-GROSS_SHPD_COST_AMT) AS Commissions
FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
   `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU
WHERE EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
AND NODE_TYPE_NM='DIGITAL'
AND rpt_lvl_0_nm  IN ('WALMART SERVICES')
GROUP BY 1,2,3,4

UNION ALL

	SELECT
		event_dt AS calendar_date,
		cast(TRIM(LEFT( TSKU.rpt_lvl_1_nm,2)) AS int64)  acctg_dept_nbr,
		TSKU.RPT_LVL_4_ID,
		FACT.CATLG_ITEM_ID,
		SUM(GROSS_SALES_REV_AMT-GROSS_SHPD_COST_AMT) AS Commissions
	FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
	   `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU
	WHERE EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
	AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
	AND NODE_TYPE_NM='DIGITAL'
	AND rpt_lvl_0_nm not IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
	GROUP BY 1,2,3,4 
	) a1 GROUP BY 1,2,3,4,5,6) a1,
    (
	SELECT
		extract(MONTH from calendar_date) AS mth1,
		extract(YEAR  from calendar_date) AS yr1,
		SUM(Commissions) AS ttl_sls
	FROM (
	SELECT
		event_dt AS calendar_date,
		SUM(GROSS_SALES_REV_AMT-GROSS_SHPD_COST_AMT) AS Commissions
	FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
	   `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU
	WHERE EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
	AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
	AND NODE_TYPE_NM='DIGITAL'
	AND rpt_lvl_0_nm  IN ('WALMART SERVICES')
	GROUP BY 1

	UNION ALL

	SELECT
		event_dt AS calendar_date,
		SUM(GROSS_SALES_REV_AMT-GROSS_SHPD_COST_AMT) AS Commissions
	FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
	   `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU
	WHERE EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
	AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
	AND NODE_TYPE_NM='DIGITAL'
	AND rpt_lvl_0_nm not IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
	GROUP BY 1 ) a1
	GROUP BY 1,2
	) a2 WHERE a1.mth1 = a2.mth1 AND   a1.yr1=a2.yr1
	GROUP BY 1,2,3,4,5,6 ) t1
	LEFT OUTER JOIN
	(
	SELECT 
		extract(YEAR from postg_dt) AS yr1, 
		extract(MONTH from postg_dt) AS mth1,
		'Services' AS ACCOUNT,
		SUM (local_crncy_amt*-1) AS SAP_Act   --Amount
	FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY  ,
	(
	SELECT
		a2.calendar_date
		FROM(SELECT 1 AS ind, cal_epoch_mo_cnt
		FROM `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a
		WHERE calendar_date = CURRENT_DATE()-33) a1,
	`wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a2
	WHERE a2.cal_epoch_mo_cnt<=a1.cal_epoch_mo_cnt
	AND a2.calendar_date between date('2024-10-26') and date('2025-02-28') -- update date
	GROUP BY 1) a2
	WHERE  postg_dt = a2.calendar_Date
	AND fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
	AND PROFIT_CNTR_NBR IN ('US09898G','US10814G')                              --For CSA
	AND rpt_acct_nbr IN (4102001,4103029,4103009,4103036,4103032,4102001,4102001,4101030,4103032,4101030,4103036,4101010)--Filter by GL Account
	GROUP BY 1,2 ) t2
	ON t1.mth1=t2.mth1 AND t1.yr1=t2.yr1
--(21$)	
--*************************************************************************************************	
	)
--(22$)	
--*************************************************************************************************

UNION ALL
--*************************************************************************************************
--(20$)	
	SELECT 
		postg_dt AS calendar_date,
		extract(MONTH from postg_dt) AS mnth1,
		extract(YEAR from postg_dt) AS yr1,
		86 AS acctg_dept_nbr,
		null as dept_subcatg_nbr,
		null as mds_fam_id,
		'S2H' AS CHANNEL,		
		NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, 
		NULL Discount_Amt,	NULL Core_Merch_Sales,	NULL FC_Exits, NULL Associate_Purchase_Discount, NULL AS Contacts, NULL AS PetRx,
		SUM (local_crncy_amt*-1) AS Services,
		NULL Core_Merch_Sales5,	NULL AS Store_Refunds, 
		'no item' as ind, 
		'core eComm' as ind2,
		20 as sno
	FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY 
	WHERE postg_dt between date('2024-10-26') and date('2025-02-28') -- update date  --Change Date here
	AND PROFIT_CNTR_NBR IN ('US09898G','US10814G')                              --For CSA
	AND rpt_acct_nbr IN (4103030,4101030,4103022,4103031,4103009,4103021,4103024)  --Filter by GL Account
	GROUP BY 1,2,3
--(20$)
--*************************************************************************************************
	

UNION ALL 
--*************************************************************************************************
--(19$)	

	SELECT 
		postg_dt AS calendar_date,
		extract(MONTH from postg_dt) AS mnth1,
		extract(YEAR from postg_dt) AS yr1,
		58 AS acctg_dept_nbr,
		null as dept_subcatg_nbr,
		null as mds_fam_id,
		'S2H' AS CHANNEL,	
		NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, 
		NULL Discount_Amt,	NULL Core_Merch_Sales,	NULL FC_Exits, NULL Associate_Purchase_Discount, NULL AS Contacts, NULL AS PetRx,
		SUM (local_crncy_amt*-1) AS Services,
		NULL Core_Merch_Sales5,	NULL AS Store_Refunds, 
		'no item' as ind, 
		'core eComm' as ind2,
		19 as sno
	FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY 
	WHERE postg_dt between date('2024-10-26') and date('2025-02-28') -- update date  --Change Date here
	AND PROFIT_CNTR_NBR IN ('US09898G','US10814G')                              --For CSA
	AND rpt_acct_nbr IN (4102003)                                    --Filter by GL Account
	GROUP BY 1,2,3
--(19$)
--*************************************************************************************************	
) a3
	GROUP BY 1,2,3,4,5,6,ind,ind2,sno
--(23$)
--*************************************************************************************************	

	

UNION ALL
/*Third Party Website*/

--*************************************************************************************************
--(18$)	
	SELECT 
		postg_dt AS calendar_date,                         --Posting Date
		extract(MONTH from postg_dt) AS mnth1,
		extract(YEAR from postg_dt) AS yr1,
		49 AS acctg_dept_nbr,
		null as dept_subcatg_nbr,
		null AS mds_fam_id,
		'S2H' AS CHANNEL,
		NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, 
		NULL Discount_Amt,	NULL Core_Merch_Sales,	NULL FC_Exits, NULL Associate_Purchase_Discount, 
		SUM (local_crncy_amt*-1) AS Contacts,
		NULL AS PetRx,NULL AS Services,	NULL Core_Merch_Sales5,	NULL AS Store_Refunds,
		'no item' as ind, 
		'core eComm' as ind2, 
		18 as sno
	FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY  
	WHERE postg_dt between date('2024-10-26') and date('2025-02-28') -- update date  --Change Date here
	AND fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
	AND PROFIT_CNTR_NBR IN ('US10504G')                              --For CSA
	AND rpt_acct_nbr IN (4101010)                                    --Filter by GL Account
	GROUP BY 1,2,3

--(18$)	
--*************************************************************************************************
		

UNION ALL
--*************************************************************************************************
--(17$)	
	SELECT 
		postg_dt AS calendar_date,                         --Posting Date
		extract(MONTH from postg_dt) AS mnth1,
		extract(YEAR from postg_dt) AS yr1,
		8 AS acctg_dept_nbr,
		null as dept_subcatg_nbr,
		null as mds_fam_id,
		'S2H' AS CHANNEL,
		NULL Virtual_value, NULL Shipping_Revenue, NULL Cash_CSA, NULL Credit_Charge_back, NULL Liquidation_Sales, NULL B2B, 
		NULL Discount_Amt,	NULL Core_Merch_Sales,	NULL FC_Exits, NULL Associate_Purchase_Discount, NULL AS Contacts,
		SUM (local_crncy_amt*-1) AS PetRx,
		NULL AS Services,	NULL Core_Merch_Sales5,	NULL AS Store_Refunds,
		'no item' as ind, 
		'core eComm' as ind2,
		17 as sno
	FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY 
	WHERE postg_dt between date('2024-10-26') and date('2025-02-28') -- update date  --Change Date here 
	AND fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
	AND PROFIT_CNTR_NBR IN ('US10504G')                              --For CSA
	AND rpt_acct_nbr IN (4101030)                                    --Filter by GL Account
	GROUP BY 1,2,3
--(17$)		
--*************************************************************************************************
	
)
GROUP BY 1,2,3,4,5,6,ind,ind2,sno
--(24$)
--*************************************************************************************************
UNION ALL

/*Added code as part of garry's changes on SIT -  10182021 */
--*************************************************************************************************
--(16$)	

SELECT 
calendar_date,
extract(MONTH from calendar_date) AS mnth1,
extract(YEAR from calendar_date) AS yr1,
acctg_dept_nbr,
RPT_LVL_4_ID,
catlg_item_id, 
NULL AS Virtual_value, NULL AS Shipping_Revenue,NULL AS Cash_CSA,NULL AS Credit_Charge_back,NULL AS Liquidation_Sales,NULL AS B2B,
NULL AS Discount_Amt,
SUM( sap*per ) AS Core_Merch_Sales,
NULL AS FC_Exits,NULL AS Associate_Purchase_Discount,NULL AS Contacts,NULL PetRx,NULL Services, NULL Core_Merch_Sales5,NULL Store_Refunds,
'online' as ind, 
'core eComm' as ind2,
16 as sno
FROM
(
SELECT
a2.calendar_date,
a2.acctg_dept_nbr,
a2.RPT_LVL_4_ID,
a2.catlg_item_id,
SUM(ttl) AS ttl,
SUM(dept_ttl/ttl) AS per,
SUM(ifnull(SAP_Act,0)*-1) AS SAP
FROM
(SELECT
Event_dt AS calendar_date,
SUM(GROSS_SALES_REV_AMT) AS ttl
  FROM   wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
  `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU
  WHERE   EVENT_DT between date('2024-10-26') and date('2025-02-28') -- update date
  AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
  AND svc_id IN (0,8)
  AND rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
  GROUP BY 1) a1,
  (SELECT
Event_dt AS calendar_date,
cast(TRIM(LEFT( TSKU.rpt_lvl_1_nm,2)) AS int64)  acctg_dept_nbr,
TSKU.RPT_LVL_4_ID,
fact.catlg_item_id,
SUM(GROSS_SALES_REV_AMT) AS dept_ttl
  FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
  `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU
  WHERE  EVENT_DT between date('2024-10-26') and date('2025-02-28') -- update date
  AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
  AND svc_id IN (0,8)
  AND rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
  GROUP BY 1,2,3,4) a2
LEFT OUTER JOIN
(SELECT 
rpt_acct_nbr,                     --GL Account Number  
acct_nm,                          --GL Account Name
postg_dt AS calendar_date,                         --Posting Date
SUM (local_crncy_amt) AS SAP_Act   --Amount
FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY
WHERE postg_dt between date('2024-10-26') and date('2025-02-28') -- update date   --Change Date here 
AND fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
AND PROFIT_CNTR_NBR IN ('US09928G')                              --For CSA
AND rpt_acct_nbr IN (4101010)                                    --Filter by GL Account
GROUP BY 1,2,3
) a3
ON a1.calendar_date =a3.calendar_date   --***********************SURYA MADE CHANGES HERE ************************************************************************************************************************************
WHERE a1.calendar_date = a2.calendar_date
GROUP BY 1,2,3,4) t1 
GROUP BY 1,2,3,4,5,6 

--(16$)
--*************************************************************************************************
	
     
) 
GROUP BY 1,2,3,4,ind,ind2,sno
)
GROUP BY 1,2,3,4,5,ind,ind2,sno

/* 1P code ending*/
		
UNION ALL

--*************************************************************************************************
--(15$)	
			SELECT
					calendar_Date,
					acctg_dept_nbr,
					DEPT_SUBCATG_NBR,
					mds_fam_id, 
					channel,
					SUM(SLS) AS REV_GMV,
--					SUM(SLS) AS REV_NS,
					ind, 
					ind2,
					sno
			FROM
					(
					
					/* FDL Net Sales*/
					
--*************************************************************************************************
--(14$)					
							SELECT VISIT_LOCAL_DT AS calendar_date 
								,ITEM.ACCTG_DEPT_NBR
								,ITEM.DEPT_SUBCATG_NBR
								,SALES.mds_fam_id
								,CASE  WHEN  (FULFMT_TYPE_ID IN (14) OR CHNL_SUBTYPE_ID IN (137,166)) THEN 'PUT' ELSE
								CASE  WHEN  (FULFMT_TYPE_ID IN (15) OR CHNL_SUBTYPE_ID IN (158,220)) THEN 'SFS' ELSE
								CASE  WHEN  CHNL_SUBTYPE_ID IN (197) THEN 'DFS' ELSE 'Other' end end end AS channel
								,SUM(SALES_AMT) AS SLS ,
								'offline' as ind, 
								'sfs-put-dlv' as ind2,
								14 as sno
								from wmt-edw-prod.US_FIN_SALES_DL_RPT_VM.WMT_STORE_SALES_ITEM_DLY_D SALES 
								LEFT OUTER JOIN wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_ITEM_DIM ITEM 
								ON ITEM.MDS_FAM_ID=SALES.MDS_FAM_ID
								WHERE ITEM.OP_CMPNY_CD='WMT-US' AND CHNL_TYPE_ID IN (30,86) 
								AND VISIT_LOCAL_DT between date('2024-10-26') and date('2025-02-28') -- update date
								AND CHNL_SUBTYPE_ID IN (207,208,137,166,158,220,197)
								GROUP BY 1,2,3,4,5
--(14$)								
--*************************************************************************************************
								

			UNION ALL

			/* In Store Returns */
--*************************************************************************************************
--(13$)
					SELECT	
							RTN.RTN_CRE_DT AS calendar_date,
							CAST(REGEXP_EXTRACT(SUBSTR(RP.RPT_HRCHY_LVL1_DESC, 0, 2), r'(\d+)') AS INT64) AS ACCTG_DEPT_NBR,	
							RP.RPT_HRCHY_LVL4_ID,
							RTN.CATLG_ITEM_ID,
							CASE 	
							    WHEN RTN.FULFMT_TYPE_DESC = 'UNSCHEDULED_DELIVERY' AND RTN.ACES_MODE_NM = 'LAST_MILE_CARRIER' THEN 'DFS'	
							    WHEN RTN.FULFMT_TYPE_DESC = 'UNSCHEDULED_DELIVERY' AND RTN.ACES_MODE_NM = 'NATIONAL_CARRIER' THEN 'SFS'	
									WHEN RTN.FULFMT_TYPE_DESC  IN ('UNSCHEDULED_PICKUP') THEN 'PUT'  ELSE 'Other' END AS Channel,		
									SUM(RTN.UNIT_PRICE * RTN.EXPC_QTY)* -1 AS SLS,
							'online' as ind, 
							'sfs-put-dlv' as ind2,
							13 as sno
					FROM wmt-edw-prod.WW_RTN_DL_VM.RTN_360_DTL RTN	
					LEFT JOIN wmt-edw-prod.WW_PRODUCT_DL_VM.PROD PRD ON  RTN.CATLG_ITEM_ID =  PRD.CATLG_ITEM_ID	
					LEFT JOIN wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_RPT_HRCHY PRH ON PRH.CATLG_ITEM_ID = PRD.CATLG_ITEM_ID 	--AND PRD.CATLG_ITEM_ID = PRH.CATLG_ITEM_ID 
					LEFT JOIN  wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_ALT_ID PIL ON PRD.CATLG_ITEM_ID = PIL.CATLG_ITEM_ID	 --AND  PIL.CATLG_ITEM_ID=PRD.CATLG_ITEM_ID  	
					AND PIL.PROD_ID_RANK_NBR = 1 AND PIL.PROD_ID_TYPE_NM = 'WUPC'	  
					LEFT JOIN  wmt-edw-prod.WW_PRODUCT_DL_VM.RPT_HRCHY RP	  ON RP.SRC_RPT_HRCHY_ID = PRH.SRC_RPT_HRCHY_ID
					WHERE RTN.RTN_CRE_DT between date('2024-10-26') and date('2025-02-28') -- update date
					AND RTN.FULFMT_TYPE_DESC IN ('UNSCHEDULED_DELIVERY','UNSCHEDULED_PICKUP') 
					AND RTN.TENANT_ORG_ID = 4571	
					AND UPPER(RTN.RTN_TYPE_DESC) IN ('DTE' ,'OPR' ,'S2S','STORE','OEX') --- All store returns - choose Returns that came through Store customer service desk	
					AND SVC_NM = "STORE_INVENTORY"	
					GROUP BY 1,2,3,4,5
					
--(13$)
--*************************************************************************************************

					)		
				WHERE CHANNEL <> 'Other' 
				GROUP BY 1,2,3,4,5,7,8,9

--(15$)
--*************************************************************************************************
					
				
				
UNION ALL

/* PHOTO */

--*************************************************************************************************
--(12$)

    SELECT 
        calendar_date,
        acctg_dept_nbr,
		RPT_LVL_4_ID,
		catlg_item_id,
        Channel,
        SUM(Sls) AS REV_GMV,
--        SUM(Sls) AS REV_NS,
		'online' as ind, 
		'core eComm' as ind2,
		12 as sno
    FROM(
        SELECT
            Event_dt AS calendar_date,
            cast(TRIM(LEFT( TSKU.rpt_lvl_1_nm,2)) AS int64)  acctg_dept_nbr,
			TSKU.RPT_LVL_4_ID,
			fact.catlg_item_id,
            'PHOTO' Channel,
            SUM(GROSS_SALES_REV_AMT+PROMO_AMT+CPM_AMT+RFND_RTL_SALES+ASSOC_DISC_AMT+GROSS_SHPG_REV_AMT) AS Sls
        FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE FACT,
        `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` TSKU        
        WHERE   EVENT_DT between date('2024-10-26') and date('2025-02-28') -- update date
        AND FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
        AND svc_id IN (7,12)
        AND rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
        GROUP BY 1,2,3,4
        ) GROUP BY 1,2,3,4,5

--(12$)
--*************************************************************************************************
		
		
UNION ALL

/* Pickup , Delivery */

--*************************************************************************************************
--(11$)
	SELECT
					calendar_date,
					acctg_dept_nbr,
					DEPT_SUBCATG_NBR,
					mds_fam_id,
					channel,
					SUM(SLS) AS REV_GMV,
--					SUM(SLS) AS REV_NS,	
					ind, 
					ind2,
					sno
			FROM
				(	
				/* FDL Net Sales*/
--*************************************************************************************************
--(10$)

				SELECT VISIT_LOCAL_DT AS calendar_date 
								,ITEM.ACCTG_DEPT_NBR
								,ITEM.DEPT_SUBCATG_NBR
								,SALES.MDS_FAM_ID
								,CASE WHEN  FULFMT_TYPE_ID IN (6,8) THEN 'Pickup' ELSE
								 CASE WHEN FULFMT_TYPE_ID IN (7,9,11,19) THEN 'Delivery' ELSE  'Other' end end  AS channel -- add evergreen
								,SUM(SALES_AMT) AS SLS
								,'offline' as ind, 
								'sfs-put-dlv' as ind2,
								10 as sno
								from wmt-edw-prod.US_FIN_SALES_DL_RPT_VM.WMT_STORE_SALES_ITEM_DLY_D SALES 
								LEFT OUTER JOIN wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_ITEM_DIM ITEM 
								ON ITEM.MDS_FAM_ID=SALES.MDS_FAM_ID
								WHERE ITEM.OP_CMPNY_CD='WMT-US' AND CHNL_TYPE_ID IN (30,86) 
								AND VISIT_LOCAL_DT between date('2024-10-26') and date('2025-02-28') -- update date
								AND FULFMT_TYPE_ID IN (6,8,7,9,11,19) -- add evergreen
								GROUP BY 1,2,3,4,5
--(10$)
--*************************************************************************************************
				
				UNION ALL
				/* In Store Returns */
--*************************************************************************************************
--(9$)
					SELECT	
							RTN.RTN_CRE_DT AS calendar_date,
							CAST(REGEXP_EXTRACT(SUBSTR(RP.RPT_HRCHY_LVL1_DESC, 0, 2), r'(\d+)') AS INT64) AS ACCTG_DEPT_NBR,	
							RP.RPT_HRCHY_LVL4_ID,
							RTN.CATLG_ITEM_ID,
							CASE 	
							    WHEN RTN.FULFMT_TYPE_DESC  IN ('EXPRESS_DELIVERY','SCHEDULED_DELIVERY','IN_HOME_DELIVERY') THEN 'Delivery'
									WHEN RTN.FULFMT_TYPE_DESC  IN ('SCHEDULED_PICKUP','EXPRESS_PICKUP') THEN 'Pickup' ELSE 'Other' END AS channel,		
									SUM(RTN.UNIT_PRICE * RTN.EXPC_QTY)* -1 AS SLS, 
							'online' as ind, 
							'sfs-put-dlv' as ind2,
							9 as sno
						FROM wmt-edw-prod.WW_RTN_DL_VM.RTN_360_DTL RTN	      
							LEFT JOIN wmt-edw-prod.WW_PRODUCT_DL_VM.PROD PRD ON  RTN.CATLG_ITEM_ID =  PRD.CATLG_ITEM_ID	
							LEFT JOIN wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_RPT_HRCHY PRH ON PRH.CATLG_ITEM_ID = PRD.CATLG_ITEM_ID --	AND PRD.CATLG_ITEM_ID = PRH.CATLG_ITEM_ID 
							LEFT JOIN  wmt-edw-prod.WW_PRODUCT_DL_VM.PROD_ALT_ID PIL ON PRD.CATLG_ITEM_ID = PIL.CATLG_ITEM_ID	-- AND  PIL.CATLG_ITEM_ID=PRD.CATLG_ITEM_ID  	
							AND PIL.PROD_ID_RANK_NBR = 1 AND PIL.PROD_ID_TYPE_NM = 'WUPC'	  
							LEFT JOIN  wmt-edw-prod.WW_PRODUCT_DL_VM.RPT_HRCHY RP	  ON RP.SRC_RPT_HRCHY_ID = PRH.SRC_RPT_HRCHY_ID	
					WHERE RTN.RTN_CRE_DT between date('2024-10-26') and date('2025-02-28') -- update date 
					AND RTN.FULFMT_TYPE_DESC IN ('EXPRESS_DELIVERY','SCHEDULED_DELIVERY','IN_HOME_DELIVERY','SCHEDULED_PICKUP','EXPRESS_PICKUP') 
					AND RTN.TENANT_ORG_ID = 4571	
					AND UPPER(RTN.RTN_TYPE_DESC) IN ('DTE' ,'OPR' ,'S2S','STORE','OEX') --- All store returns - choose Returns that came through Store customer service desk	
					AND SVC_NM = "STORE_INVENTORY"	
					GROUP BY 1,2,3,4,5
					
--(9$)
--*************************************************************************************************
				
				UNION ALL


--*************************************************************************************************
--(8$)
					SELECT        
							VISIT_LOCAL_DT AS CALENDAR_DATE,
							ITEM.ACCTG_DEPT_NBR,
							ITEM.dept_subcatg_nbr,
							ITEM.mds_fam_id,
							'Delivery' AS CHANNEL,
							SUM(SALES_AMT) SLS , 
							'offline' as ind, 
							'sfs-put-dlv' as ind2,
							8 as sno
					FROM `wmt-edw-prod.US_WM_FIN_SALES_DL_VM.WMT_STORE_SALES_DTL` AS FACT
					/*INNER JOIN wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_ITEM_DIM_HIST AS ITEM
					ON FACT.ITEM_CURR_KEY=ITEM.ITEM_CURR_KEY*/    
					INNER JOIN wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_ITEM_DIM AS ITEM
					ON FACT.MDS_FAM_ID=ITEM.MDS_FAM_ID
					/*INNER JOIN `wmt-edw-prod.WW_FIN_DL_VM.FIN_DEPT_DIM_HIST` AS DEPT
					ON FACT.FIN_CURR_DEPT_KEY=DEPT.FIN_CURR_DEPT_KEY*/
					WHERE FACT.OTHER_INCOME_IND = 0 AND FACT.INSTACART_IND = 1 
					AND VISIT_LOCAL_DT BETWEEN date('2024-10-26') and date('2025-02-28') -- update date 
					GROUP BY 1,2,3,4
--(8$)
--*************************************************************************************************
	) 
	WHERE channel not IN ('Other')
	GROUP BY 1,2,3,4,5,7,8,sno 

	/* UNION ALL

--*************************************************************************************************
--(7$)
		-- Add Instacart Plan for D-1
		SELECT
			RPT_DT AS CALENDAR_DATE,
			CAST(REGEXP_EXTRACT(SUBSTR(SPC.super_dept_nm, 0, 2), r'(\d+)') as INT64) AS ACCTG_DEPT_NBR,
			SUB_CATEG_ID as dept_subcatg_id,
			null as mds_fam_id,
			CASE WHEN RPT_CHNL_NM IN ('INSTACART') THEN 'Delivery' ELSE 'Other' END AS CHANNEL,
			SUM(SALES) as SLS ,
			'no item' as ind, 
			'sfs-put-dlv' as ind2, 
			7 as sno
    FROM `wmt-edw-prod.WW_GEC_VM.SALES_PROD_CAT_PLN_FCST` SPC
    JOIN
        (SELECT A.ORG_ID, B.WM_DSTRBTR_NO
         FROM `wmt-edw-prod.WW_GEC_VM.ORG_ACCESS_POINT` A
         JOIN `wmt-edw-prod.WW_GEC_VM.ORG_SHIP_NODE` B
         ON A.PARENT_ORG_ID = B.ORG_ID
         WHERE (A.ORG_TYPE_ID = 1300 OR (A.ORG_TYPE_ID = 1301 AND LENGTH(A.SRC_ORG_CD) >=8 AND LENGTH(COALESCE(A.SRC_ORG_KEY,'1')) >10 ) )
         group by 1,2) OAN
    ON OAN.ORG_ID = SPC.ACCESS_POINT_ORG_ID
    WHERE RPT_CHNL_NM IN ('INSTACART') and RPT_DT = CURRENT_DATE()-1 AND SUMM_TYPE_TXT='OPD_PLAN' 
    GROUP BY 1,2,3,5
--(7$)
--*************************************************************************************************	 

*/
--(11$)	
 --*************************************************************************************************
 
UNION ALL 

/* PHARMACY & Pharmacy_Txt*/
/*Added Pharmacy Txt data along wiht Pharmacy WEB based on Garry's request*/
--*************************************************************************************************
--(6$)

		SELECT
			CAST(B.CAL_DT AS DATE) AS calendar_date,
			38 AS acctg_dept_nbr,
			null as RPT_LVL_4_ID,
			null as catlg_item_id,
			CHNL AS channel,
			SUM(REV_GMV) AS REV_GMV,
--			SUM(REV_NS) AS REV_NS,
			'no item' AS IND, 
			'sfs-put-dlv' as ind2, 
			6 as sno
		FROM `wmt-fin-fcp-prod-ds-s4.WW_FIN_FCP_ADHOC.ECOMM_DSR_REV_FINAL` A
		JOIN `wmt-fin-fcp-prod-ds`.WW_FIN_FCP_TABLES.FCP_CALENDAR_DIM  B
		ON A.CAL_KEY = B.CAL_KEY
		WHERE CAST(B.CAL_DT AS DATE) between date('2024-10-26') and date('2025-02-28') -- update date
		AND CHNL IN ('PHARMACY', 'Pharmacy_Txt')
		GROUP BY CAST(B.CAL_DT AS DATE) ,CHNL	
 
 --(6$)
 --*************************************************************************************************
  UNION ALL
  /*MP GMV*/

--*************************************************************************************************
 --(5$)
		SELECT 
			calendar_date,
			acctg_dept_nbr,
			RPT_LVL_4_ID,
			prmry_sku_id as catlg_item_id,
			channel,
			SUM(REV_GMV) AS REV_GMV,
--			SUM(REV_NS)  AS REV_NS, 
			'online' as ind, 
			'MP' as ind2, 
			sno
		FROM(
		SELECT 
			  sls.rpt_shpd_based_dt AS calendar_date,
			  CASE WHEN rpt_lvl_0_nm IN ('UNASSIGNED','UNASSIGNED L0') THEN 0 ELSE CAST(TRIM(LEFT( rpt_lvl_1_nm,2)) AS int64) END AS  acctg_dept_nbr,
			  RPT_LVL_4_ID,
			  cast(prmry_sku_id as int64) as prmry_sku_id,
			  svc_nm as channel,
			  SUM (gmv_amt) AS REV_GMV,
--			  0 AS REV_NS, 
        'online' as ind, 
				'MP' as ind2,
			  5 as sno
          FROM  wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_EVENT_BASED_SALES 	sls	 /* Replaced ols EBS to new EBS table on */ 
		  WHERE gmv_cr_ind = 1  AND gmv_ind = 1  AND  op_cmpny_cd = 'WMT.COM'  AND svc_nm IN ('MP')
		  AND prmry_sku_id <> '-999'
		  AND sls.rpt_shpd_based_dt between date('2024-10-26') and date('2025-02-28') -- update date
		  AND IFNULL(vend_nbr,'0') not IN ('285986')
		  AND rpt_lvl_0_nm NOT IN ('SERVICES','WALMART SERVICES') -- Removed UNASSIGNED, UNASSIGNED L0' from the filter based on Amitesh request-20211216
		  GROUP BY 1,2,3,4,5
			

  
UNION ALL
/*MP Comm*/
--*************************************************************************************************
--(4$)

SELECT
				calendar_date,
				acctg_dept_nbr,
				RPT_LVL_4_ID,
				catlg_item_id,
				'MP' AS channel,
				0 AS REV_GMV,
--			SUM(value) AS REV_NS, 
			  'online' as ind, 
				'MP' as ind2,
				sno
			FROM
			(SELECT
					calendar_date,
					acctg_dept_nbr,
					RPT_LVL_4_ID,
					catlg_item_id,
					'Marketplace_Commissions' AS account,
					SUM(CASE WHEN calendar_date = CAST(CURRENT_DATE()-1 AS DATE) THEN ttl*per
                             WHEN calendar_date = CAST(CURRENT_DATE()-2 AS DATE) AND SAP <= 500000 THEN ttl*per 
                             WHEN calendar_date = CAST(CURRENT_DATE()-3 AS DATE) AND SAP <= 500000 THEN ttl*per 
                             WHEN calendar_date = CAST(CURRENT_DATE()-4 AS DATE) AND SAP <= 500000 THEN ttl*per                      
                             ELSE sap*per end)  AS value, 
        'online' as ind, 
				'MP' as ind2,
				sno
			FROM
			(SELECT
					a2.calendar_date,
					a2.acctg_dept_nbr,
					RPT_LVL_4_ID,
					catlg_item_id,
					SUM(ttl) AS ttl,
					SUM(dept_ttl/ttl) AS per,
					SUM(ifnull(SAP_Act,0)) AS SAP, 
					sno
			FROM
			(SELECT 
					EVENT_DT AS calendar_date,
					SUM(MP_COMM_AMT) AS  ttl
			FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE AS FACT
			LEFT JOIN wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU TSKU
			ON FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
			WHERE EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
            AND FACT.SVC_ID IN (10)
			AND rpt_lvl_0_nm NOT IN ('SERVICES','WALMART SERVICES','UNASSIGNED','UNASSIGNED L0')
			AND  IFNULL(OWN_PRMRY_VEND_ID,'0') NOT IN ('285986')
			GROUP BY 1) a1,
			(SELECT 
					EVENT_DT AS calendar_date,
					CAST(TRIM(LEFT( TSKU.rpt_lvl_1_nm,2)) AS int64)  AS acctg_dept_nbr,
					TSKU.RPT_LVL_4_ID,
					fact.catlg_item_id,
					SUM(MP_COMM_AMT) AS  dept_ttl, 
					4 as sno
			FROM wmt-edw-prod.US_FIN_ECOMM_DL_VM.FIN_ACCT_SUMM_OMNI_LITE AS FACT
			LEFT JOIN wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU TSKU
			ON FACT.CATLG_ITEM_ID = TSKU.CATLG_ITEM_ID
			WHERE EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
            AND FACT.SVC_ID IN (10)
			AND rpt_lvl_0_nm NOT IN ('SERVICES','WALMART SERVICES','UNASSIGNED','UNASSIGNED L0') 
			AND  IFNULL(OWN_PRMRY_VEND_ID,'0') NOT IN ('285986')
			GROUP BY 1,2,3,4) a2
			LEFT OUTER JOIN
			(SELECT 
					postg_dt AS calendar_date,    
					'Commissions' AS ACCOUNT,
					SUM (local_crncy_amt*-1) AS SAP_Act   --Amount
			FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY
			WHERE postg_dt between date('2024-10-26') and date('2025-02-28') -- update date   --Change Date here 
			AND fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
			AND PROFIT_CNTR_NBR IN ( 'US10596G' ,'US10595G') --For CSA- Added additional Profit Center as per Garry Williamson on 12/13/2021
			AND rpt_acct_nbr IN (4101030,4101091)                                    --Filter by GL Account
            AND manual_txt  IN ('MP MP_Net_Subsidy','MP MP_Gross_Commission','MP MP_Net_Commission','MP Service Income (commission)','MPServiceIncome(commission)')
			GROUP BY 1 ) a3
			ON a2.calendar_date =a3.calendar_date
			WHERE a1.calendar_date = a2.calendar_date
			GROUP BY 1,2,3,4,8) t1
			GROUP BY 1,2,3,4,sno

	    UNION ALL
/*#######  Contra Sales ###########*/
--*************************************************************************************************
--(3$)
SELECT
				calendar_date,
				acctg_dept_nbr,
				RPT_LVL_4_ID,
				catlg_item_id,
				'MP' AS channel,
				0 AS REV_GMV,
   --  		    CASE WHEN calendar_date = CURRENT_DATE - 1 AND SUM(value) = 0 THEN AVG(SUM(value)) OVER (PARTITION BY acctg_dept_nbr ORDER BY calendar_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)  
		--		     ELSE SUM(value) END AS REV_NS,
			    'online' as ind, 
				'MP' as ind2,
				2 as sno
			FROM
			(
			SELECT
					t1.calendar_date,
					t1.acctg_dept_nbr,
					RPT_LVL_4_ID,
					catlg_item_id,
					t1.sls,
					t1.ttl_sls,
					t2.SAP_Act,
					CASE WHEN SAP_Act is NULL THEN sls ELSE sls/ttl_sls*sap_act END AS Value
			FROM
			(
			SELECT
					calendar_date,
					a1.mth1,
					a1.yr1,
					acctg_dept_nbr,
					RPT_LVL_4_ID,
					catlg_item_id,
					SUM(sls) AS sls,
					SUM(ttl_sls) AS ttl_sls
			FROM
			(
			SELECT
				event_dt AS calendar_date, 
				extract(MONTH from event_dt) AS mth1,
				extract(YEAR from event_dt) AS yr1,
				cast(TRIM(LEFT( b.rpt_lvl_1_nm,2)) AS int64)  acctg_dept_nbr,
				b.RPT_LVL_4_ID,
				b.catlg_item_id,
				SUM(fulfmt_chrg_amt) AS sls
			from wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_SVC_CHRG_EVENT a,
--			FROM wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_CHRG_EVENT A,
			 `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` b 
			 WHERE CAST(a.CATLG_ITEM_ID AS string) = b.prmry_sku_id 
			AND EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
			AND b.op_cmpny_cd IN ('WMT.COM')
			AND  rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
			GROUP BY 1,2,3,4,5,6) a1,
			(
			SELECT
					extract(MONTH from event_dt) AS mth1,
					extract(YEAR from event_dt) AS yr1,
					SUM(fulfmt_chrg_amt) AS ttl_sls
			from wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_SVC_CHRG_EVENT a,
--			FROM wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_CHRG_EVENT A,
			 `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` b 
			 WHERE	 CAST(a.CATLG_ITEM_ID AS string) = b.prmry_sku_id 
			AND EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
			AND b.op_cmpny_cd IN ('WMT.COM')
			AND  rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
			GROUP BY 1,2) a2
			WHERE	a1.mth1 = a2.mth1
			AND a1.yr1=a2.yr1
			GROUP BY 1,2,3,4,5,6
			) t1
			LEFT OUTER JOIN
			(
			SELECT 
				extract(YEAR from postg_dt) AS yr1, 
				extract(MONTH from postg_dt) AS mth1,
				'WFS' AS ACCOUNT,
				SUM (local_crncy_amt*-1) AS SAP_Act   --Amount
			FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY ,
			(
			SELECT a2.calendar_date
			FROM
			(SELECT 1 AS ind, cal_epoch_mo_cnt
			FROM `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a
			WHERE calendar_date = CURRENT_DATE()-33) a1,
			`wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a2
			WHERE a2.cal_epoch_mo_cnt<=a1.cal_epoch_mo_cnt
			AND a2.calendar_date between date('2024-10-26') and date('2025-02-28') -- update date
			GROUP BY 1) a2
			WHERE postg_dt = a2.calendar_Date
			AND fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
			AND PROFIT_CNTR_NBR = 'US09074G'                              --For CSA
			AND rpt_acct_nbr IN (4101030,4104001,4101130)                                    --Filter by GL Account
			GROUP BY 1,2,3
			) t2
			ON t1.mth1=t2.mth1 AND t1.yr1=t2.yr1
			) 
            GROUP BY 1,2,3,4
--(3$)
--*************************************************************************************************			
            )
            GROUP BY 1,2,3,4,9
--(4$)
--*************************************************************************************************			
		) GROUP BY 1,2,3,4,5,sno
--(5$)
--*************************************************************************************************	
/*##########   WFS    ############*/
UNION ALL
--*************************************************************************************************
--(2$)
			SELECT
				calendar_date,
				acctg_dept_nbr,
				RPT_LVL_4_ID,
				catlg_item_id,
				'MP' AS channel,
				0 AS REV_GMV,
   --  		    CASE WHEN calendar_date = CURRENT_DATE - 1 AND SUM(value) = 0 THEN AVG(SUM(value)) OVER (PARTITION BY acctg_dept_nbr ORDER BY calendar_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)  
   --				     ELSE SUM(value) END AS REV_NS,
			    'online' as ind, 
				'MP' as ind2,
				2 as sno
			FROM
			(
			SELECT
					t1.calendar_date,
					t1.acctg_dept_nbr,
					RPT_LVL_4_ID,
					catlg_item_id,
					t1.sls,
					t1.ttl_sls,
					t2.SAP_Act,
					CASE WHEN SAP_Act is NULL THEN sls ELSE sls/ttl_sls*sap_act END AS Value
			FROM
			(
			SELECT
					calendar_date,
					a1.mth1,
					a1.yr1,
					acctg_dept_nbr,
					RPT_LVL_4_ID,
					catlg_item_id,
					SUM(sls) AS sls,
					SUM(ttl_sls) AS ttl_sls
			FROM
			(
			SELECT
				event_dt AS calendar_date, 
				extract(MONTH from event_dt) AS mth1,
				extract(YEAR from event_dt) AS yr1,
				cast(TRIM(LEFT( b.rpt_lvl_1_nm,2)) AS int64)  acctg_dept_nbr,
				b.RPT_LVL_4_ID,
				b.catlg_item_id,
				SUM(fulfmt_chrg_amt) AS sls
			from wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_SVC_CHRG_EVENT a,
			
--			FROM wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_CHRG_EVENT A,
			 `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` b 
			 WHERE CAST(a.CATLG_ITEM_ID AS string) = b.prmry_sku_id 
			AND EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
			AND b.op_cmpny_cd IN ('WMT.COM')
			AND  rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
			GROUP BY 1,2,3,4,5,6) a1,
			(
			SELECT
					extract(MONTH from event_dt) AS mth1,
					extract(YEAR from event_dt) AS yr1,
					SUM(fulfmt_chrg_amt) AS ttl_sls
			from wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_SVC_CHRG_EVENT a,
			
--			FROM wmt-edw-prod.WW_SUPPLY_CHAIN_DL_VM.FULFMT_CHRG_EVENT A,
			 `wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_TACTICAL_SKU` b 
			 WHERE	 CAST(a.CATLG_ITEM_ID AS string) = b.prmry_sku_id 
			AND EVENT_DT  between date('2024-10-26') and date('2025-02-28') -- update date
			AND b.op_cmpny_cd IN ('WMT.COM')
			AND  rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
			GROUP BY 1,2) a2
			WHERE	a1.mth1 = a2.mth1
			AND a1.yr1=a2.yr1
			GROUP BY 1,2,3,4,5,6
			) t1
			LEFT OUTER JOIN
			(
			SELECT 
				extract(YEAR from postg_dt) AS yr1, 
				extract(MONTH from postg_dt) AS mth1,
				'WFS' AS ACCOUNT,
				SUM (local_crncy_amt*-1) AS SAP_Act   --Amount
			FROM wmt-edw-prod.US_FIN_SAP_DL_RPT_SECURE.US_SAP_GL_MTL_FCP_DST_DLY ,
			(
			SELECT a2.calendar_date
			FROM
			(SELECT 1 AS ind, cal_epoch_mo_cnt
			FROM `wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a
			WHERE calendar_date = CURRENT_DATE()-33) a1,
			`wmt-edw-prod.US_CORE_DIM_VM.CALENDAR_DIM` a2
			WHERE a2.cal_epoch_mo_cnt<=a1.cal_epoch_mo_cnt
			AND a2.calendar_date between date('2024-10-26') and date('2025-02-28') -- update date
			GROUP BY 1) a2
			WHERE postg_dt = a2.calendar_Date
			AND fin_cmpny_cd IN ('A148','A217')                             --Filter by Company Code
			AND PROFIT_CNTR_NBR = 'US09074G'                              --For CSA
			AND rpt_acct_nbr IN (4101030,4104001,4101130)                                    --Filter by GL Account
			GROUP BY 1,2,3
			) t2
			ON t1.mth1=t2.mth1 AND t1.yr1=t2.yr1
			) 
            GROUP BY 1,2,3,4
--(2$)
--*************************************************************************************************
)
GROUP BY 1,2,3,4,5,ind,ind2,sno
)
Group by 1,2,3,4,5,6,ind,ind2,sno
)
Group by 1,2,3,4,5,6,ind,ind2,sno
) A

UNION ALL
--*************************************************************************************************
--(1$) 
SELECT
		CAST(replace(CAST(rpt_dt  as STRING), '-', '') AS INT64) AS CAL_KEY,
		CAST(TRIM(LEFT(L1_NM,2)) AS INT64)  AS ACCTG_DEPT_NBR,
		rpt_lvl_4_id,
		catlg_item_id,
		RPT_DT,
		Channel AS CHNL,
		SUM(0) AS GMV_ACTL,
--		SUM(MP_Commissions) AS MP_Commissions,
--		SUM(Units) AS UNITS,
--		SUM(IMU) AS IMU,
--		SUM(CP) AS CP,
--		SUM(GMV_Calc_Amt) AS GMV_CALC_AMT,
--		SUM(0) AS NET_SALES_ACTL,
		'online' as ind, 
		'MP' as ind2,
		1 as sno
	FROM 
	(
  
	 SELECT 
		  sls.rpt_shpd_based_dt AS rpt_dt,
		  rpt_lvl_0_nm AS L0_NM,
		  CASE WHEN svc_nm = 'PHARMACY' THEN '38'  WHEN  rpt_lvl_2_nm = 'PHOTO WEB INITIATED' THEN '85' ELSE rpt_lvl_1_nm end AS L1_NM,
		  rpt_lvl_4_id,
		  cast(prmry_sku_id as int64) as catlg_item_id,
		  svc_nm AS Channel,
		  SUM (gmv_amt) AS GMV,
	--	  0 AS MP_Commissions,
	--	  SUM(shpd_qty) AS Units,
	--	  SUM(0) AS IMU,
	--	  SUM(0) AS CP,
	--	  SUM(0) AS GMV_Calc_Amt,
	--	  SUM(net_sales_amt) AS Net_Sales, 
		  'online' as ind
          FROM  wmt-edw-prod.WW_CREW_DL_RPT_VM.CNSLD_EVENT_BASED_SALES 	sls	 
	  WHERE   gmv_cr_ind = 1  AND gmv_ind = 1  AND  op_cmpny_cd = 'WMT.COM'  AND svc_nm IN ('MP','PHARMACY')  AND prmry_sku_id <> '-999'
	  AND sls.rpt_shpd_based_dt between date('2024-10-26') and date('2025-02-28') -- update date
	  AND IFNULL(vend_nbr,'0') not IN ('285986')
	  AND rpt_lvl_0_nm NOT IN ('SERVICES','UNASSIGNED','UNASSIGNED L0','WALMART SERVICES')
	  GROUP BY 1,2,3,4,5,6
	  

	)	GROUP BY 1,2,3,4,5,6  
--(1$)
--*************************************************************************************************
)
GROUP BY 1,2,3,4,5,6,ind,ind2,sno
);


-- Online Sales to Offline Hierarchies--
create or replace table wmt-mint-mmr-mw-prod.mw_numerator_dev.online_sales_FY26Feb_breakout as -- update name
with
b as (select * from wmt-mint-mmr-mw-prod.mw_numerator_dev.online_sales_FY26Feb_pre where ind in ('online', 'no item')), --update name
g as (select * from wmt-mint-mmr-mw-prod.mw_numerator_dev.online_sales_FY26Feb_pre where ind in ('offline')), --update name
d as (select cal_dt, FISCAL_YR_NM, WM_FULL_YR_NBR, WM_MTH_NM, WM_MTH_NBR, wm_yr_wk_nbr  from wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_CALENDAR_DIM where geo_region_cd='US'), 
f as (select mds_fam_id, lpad(cast(upc_nbr as string),13,'0') as upc_nbr, acctg_dept_nbr, acctg_dept_desc, MDSE_SEG_NBR, MDSE_SEG_DESC, MDSE_SUBGROUP_NBR, MDSE_SUBGROUP_DESC, 
dept_nbr, dept_desc, dept_catg_grp_nbr, dept_catg_grp_desc, DEPT_CATG_NBR, dept_catg_desc, DEPT_SUBCATG_NBR, dept_subcatg_desc, brand_id, brand_nm, vendor_nbr
from wmt-edw-prod.WW_CORE_DIM_DL_VM.DL_ITEM_DIM where op_cmpny_cd='WMT-US'), 

h as (select CATLG_ITEM_ID, 
ACCTG_DEPT_DESC,
ACCTG_DEPT_NBR,
cast(DIV_ID as string) DIV_ID,
DIV_NM,
cast(SUPER_DEPT_ID as string) SUPER_DEPT_ID,
SUPER_DEPT_NM,
cast(DEPT_ID as string) DEPT_ID,
DEPT_NM,
cast(CATEG_ID as string) CATEG_ID,
CATEG_NM,
cast(SUB_CATEG_ID as string) SUB_CATEG_ID,
SUB_CATEG_NM from wmt-edw-prod.WW_FIN_DL_VM.WM_ECOMM_PROD_HIER_D),

i as (select
ACCTG_DEPT_NBR,
ACCTG_DEPT_DESC,
cast(DIV_ID as string) DIV_ID,
DIV_NM,
cast(SUPER_DEPT_ID as string) SUPER_DEPT_ID,
SUPER_DEPT_NM,
cast(DEPT_ID as string) DEPT_ID,
DEPT_NM,
cast(CATEG_ID as string) CATEG_ID,
CATEG_NM,
cast(SUB_CATEG_ID as string) SUB_CATEG_ID,
SUB_CATEG_NM,
b.STORE_DEPT_CATG_GRP_NBR,
b.STORE_DEPT_CATG_GRP_DESC,
c.STORE_DEPT_CATG_NBR,
c.STORE_DEPT_CATG_DESC,
d.STORE_DEPT_SUBCATG_NBR,
d.STORE_DEPT_SUBCATG_DESC
FROM wmt-edw-prod.WW_FIN_DL_VM.WM_ECOMM_PROD_HIER_D a
left join wmt-edw-prod.WW_FIN_DL_VM.FIN_OMNI_HIER_MAP b
on a.dept_id = b.ECOMM_DEPT_CATG_GRP_NBR
and b.HIER_LVL_NM = 'DEPT_CATG_GRP'
left join wmt-edw-prod.WW_FIN_DL_VM.FIN_OMNI_HIER_MAP c
on a.categ_id = c.ECOMM_DEPT_CATG_NBR
and c.HIER_LVL_NM = 'DEPT_CATG'
left join wmt-edw-prod.WW_FIN_DL_VM.FIN_OMNI_HIER_MAP d
on a.sub_categ_id = d.ECOMM_DEPT_SUBCATG_NBR
and d.HIER_LVL_NM = 'DEPT_SUBCATG'
Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
Order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)

select b.cal_key, b.rpt_dt, d.fiscal_yr_nm, d.wm_full_yr_nbr, d.wm_mth_nm, d.wm_mth_nbr, d.wm_yr_wk_nbr, 
b.catlg_item_id,--h.upc_nbr, h.gtin, h.prod_nm, h.prod_type_nm, --
b.acctg_dept_nbr as fdl_acctg_dept_nbr, 
h.acctg_dept_nbr, 
h.acctg_dept_desc,
i.STORE_DEPT_CATG_GRP_NBR as DEPT_CATG_GRP_NBR,
i.STORE_DEPT_CATG_GRP_DESC as DEPT_CATG_GRP_DESC,
i.STORE_DEPT_CATG_NBR as DEPT_CATG_NBR,
i.STORE_DEPT_CATG_DESC as DEPT_CATG_DESC,
i.STORE_DEPT_SUBCATG_NBR as DEPT_SUBCATG_NBR,
i.STORE_DEPT_SUBCATG_DESC as DEPT_SUBCATG_DESC,
b.chnl, 
b.gmv_actl, --b.actl_units_sold, b.net_sales_actl, 
ind, 
ind2, 
sno,
CASE WHEN BRAND_ID IN (437122,540251,209702,272035,441247,391786,499475,537981,541119,385054,461574,540250,541379,541543,552747,404125,537982,498801,540268,552735,475399,541252,453444,446143,540292,280672,416375,416376,416377,430158,416378,400098,423131,416379,416380,416381,416382,416383,416384,416385,416386,416387,482152,279904,540297,460104,541120,460105,450482,553992,496841,440544,540293,134167,499127,506658,441993,209719,296071,509558,532447,545015,552708,401887,482153,541402,482151,540270,387459,451855,553778,372871,323546,206398,480596,480597,480598,480599,476966,206397,440543,451592,435003,278082,427268,283525,476967,554027,315458,457998,552566,550582,451895,458441,440047,209695,540269,206399,480603,550569,388885,452261,451904,541542,476965,552709,550580,541253,543793,439429,537790,540888,546855,272057,310995,449266,512777,481455,482064,325838,443842,401886,209722,542912,119436,542914,240609,97261,241747,247297,213133,410623,449262,3333,534060,542913,209697,455152,436181,484126,176751,510492,436761,206395,451214,455154,381353,211292,428133,234503,381697,428139,453521,435993,482215,414041,542579,409476,415789,486189,365085,434017,299134,198525,241742,561673,561674,448443,556369,388693,571782,561046,561047,561042,561040,561041,561043,450492,561048,561049,561045,561044,561438,450492,572858,572859) OR BRAND_NM IN ('719 Walnut Avenue','719 West','Aquaculture','Assurance','Bella Bolle','Belle','Bleu Clair Vodka','Bocholt Lager','Caged','Caliber','California Dream','Cellar Box','Cellar Craft','Cellar Four79','Che Grande','Clear American','Cliff Top Bock','Colville Bourbon','Commanders','Coral Cove Rum','Country Farmhouse','Di Marco','Dlfl','Dominant 7','Douhans','Equate','Equate Act','Equate Act Kids','Equate Basic','Equate Beauty','Equate Floss','Equate Hair','Equate Hbl','Equate Kids','Equate List Adv','Equate Oral Pain','Equate Plax','Equate Pro Hlth','Equate Regular','Equate Scope','Equate Totalcare','Equate Whitening','Esto Es','Exerhides','Fachero','Fifth Wheel','Five Acres','Flor Azul','Flower Bed','Freshness Guaranteed','Gaida','Golden Rewards','Granndach','Great Value','Great Value Reg Ygrt','Heir To The Throne','Holiday','Holiday Time','Holiday -Wal-Mart','Import Vaps','La Moneda','Laurendeaux','Lavila','Lucky Duck','Lucotto','Lunar Harvest','Luxury Edition','Mad Hen','Marketside','Mixed Up','Mullins','Oak Leaf Brand','Ol Roy','One Source','Opp15','Opp17','Opp28','Opp9','Pacific Drift','Parents Choice','Pet All Star','Phantom Bay','Price First','Prima Della','Pure Balance','Reli On','Rockdale','Salinan','Sams Choice','Sams Cola','Sierra Sangria','Skyfair','Solana','Spark','Spark.Create.Imagine','Special Kitty','Speckled Tail','Spring Valley','Stainless','Stedmans Select','Swish','Taproot','The Grifter','Three Ghost Vine','Trouble Brewing','Un Double','Unscripted','Vecherinka','Vibrant Life','Vibrant Life Food','Vintage Crush','Waikiwi Bay','Wall Art','Walmart','Wal-Mart Bakery','Walmart Deli','Walmart Produce','Walmart Seafood','Whispering Hills','White Cloud','Wild Oats','World Table','Mainstays','Time And Tru','No Boundaries','Wonder Nation','George','Athletic Works','Everstart','Ozark Trail','Faded Glory','Onn','Hyper Tough','Secret Treasures','Pen + Gear','Terra&Sky','Super Tech','Blackweb','Auto Drive','Way To Celebrate','Brahma','Expert Grill','Adventure Force','Kid Connection','Play Day','Hotel Style','Your Zone','Colorplace','My Life As','Tred Safe','Protâgâ','My Sweet Love','Chapter','Kidde','Walmart Grill','Adventure Wheels','Fun 2 Bake','Faded Glory Leather','Backyard Grill','Mainstays Kids','Russell','Swiss Tech','Danskin Now','Garanimals','Bhg','Fall And Winter Wm','Fetchwear Wm','Value','Fieldpack Marketside','Good N Clean','Rumgria','Bloke','Collini','Cruin','Endroit','Flusso','Fruttuosa','Hinnant','Loin','Salvare','Cher','Volca','Grani Lambrusco','Hinnant','Viamora','Winemakers Selection','Mcclaren Farms') THEN "PB" ELSE "OTHER" END AS brand_breakout
from b
left outer join d on b.rpt_dt=d.cal_dt 
left outer join h on h.catlg_item_id=b.catlg_item_id
left outer join i on h.DIV_ID = i.DIV_ID 
and h.SUPER_DEPT_ID = i.SUPER_DEPT_ID 
and h.DEPT_ID = i.DEPT_ID 
and h.CATEG_ID = i.CATEG_ID 
and h.SUB_CATEG_ID = i.SUB_CATEG_ID
left join f on b.catlg_item_id = f.mds_fam_id

union all 

select g.cal_key, g.rpt_dt, d.fiscal_yr_nm, d.wm_full_yr_nbr, d.wm_mth_nm, d.wm_mth_nbr, d.wm_yr_wk_nbr, 
g.catlg_item_id, --f.upc_nbr, null as gtin, null as prod_nm, null as prod_type_nm, --
g.acctg_dept_nbr as fdl_acctg_dept_nbr,
f.acctg_dept_nbr, acctg_dept_desc,
f.dept_catg_grp_nbr,
f.dept_catg_grp_desc,
f.dept_catg_nbr,
f.dept_catg_desc,
f.dept_subcatg_nbr, 
f.dept_subcatg_desc,
g.chnl, g.gmv_actl,
ind, ind2, sno,
-- from 
CASE WHEN BRAND_ID IN (437122,540251,209702,272035,441247,391786,499475,537981,541119,385054,461574,540250,541379,541543,552747,404125,537982,498801,540268,552735,475399,541252,453444,446143,540292,280672,416375,416376,416377,430158,416378,400098,423131,416379,416380,416381,416382,416383,416384,416385,416386,416387,482152,279904,540297,460104,541120,460105,450482,553992,496841,440544,540293,134167,499127,506658,441993,209719,296071,509558,532447,545015,552708,401887,482153,541402,482151,540270,387459,451855,553778,372871,323546,206398,480596,480597,480598,480599,476966,206397,440543,451592,435003,278082,427268,283525,476967,554027,315458,457998,552566,550582,451895,458441,440047,209695,540269,206399,480603,550569,388885,452261,451904,541542,476965,552709,550580,541253,543793,439429,537790,540888,546855,272057,310995,449266,512777,481455,482064,325838,443842,401886,209722,542912,119436,542914,240609,97261,241747,247297,213133,410623,449262,3333,534060,542913,209697,455152,436181,484126,176751,510492,436761,206395,451214,455154,381353,211292,428133,234503,381697,428139,453521,435993,482215,414041,542579,409476,415789,486189,365085,434017,299134,198525,241742,561673,561674,448443,556369,388693,571782,561046,561047,561042,561040,561041,561043,450492,561048,561049,561045,561044,561438,450492,572858,572859) OR BRAND_NM IN ('719 Walnut Avenue','719 West','Aquaculture','Assurance','Bella Bolle','Belle','Bleu Clair Vodka','Bocholt Lager','Caged','Caliber','California Dream','Cellar Box','Cellar Craft','Cellar Four79','Che Grande','Clear American','Cliff Top Bock','Colville Bourbon','Commanders','Coral Cove Rum','Country Farmhouse','Di Marco','Dlfl','Dominant 7','Douhans','Equate','Equate Act','Equate Act Kids','Equate Basic','Equate Beauty','Equate Floss','Equate Hair','Equate Hbl','Equate Kids','Equate List Adv','Equate Oral Pain','Equate Plax','Equate Pro Hlth','Equate Regular','Equate Scope','Equate Totalcare','Equate Whitening','Esto Es','Exerhides','Fachero','Fifth Wheel','Five Acres','Flor Azul','Flower Bed','Freshness Guaranteed','Gaida','Golden Rewards','Granndach','Great Value','Great Value Reg Ygrt','Heir To The Throne','Holiday','Holiday Time','Holiday -Wal-Mart','Import Vaps','La Moneda','Laurendeaux','Lavila','Lucky Duck','Lucotto','Lunar Harvest','Luxury Edition','Mad Hen','Marketside','Mixed Up','Mullins','Oak Leaf Brand','Ol Roy','One Source','Opp15','Opp17','Opp28','Opp9','Pacific Drift','Parents Choice','Pet All Star','Phantom Bay','Price First','Prima Della','Pure Balance','Reli On','Rockdale','Salinan','Sams Choice','Sams Cola','Sierra Sangria','Skyfair','Solana','Spark','Spark.Create.Imagine','Special Kitty','Speckled Tail','Spring Valley','Stainless','Stedmans Select','Swish','Taproot','The Grifter','Three Ghost Vine','Trouble Brewing','Un Double','Unscripted','Vecherinka','Vibrant Life','Vibrant Life Food','Vintage Crush','Waikiwi Bay','Wall Art','Walmart','Wal-Mart Bakery','Walmart Deli','Walmart Produce','Walmart Seafood','Whispering Hills','White Cloud','Wild Oats','World Table','Mainstays','Time And Tru','No Boundaries','Wonder Nation','George','Athletic Works','Everstart','Ozark Trail','Faded Glory','Onn','Hyper Tough','Secret Treasures','Pen + Gear','Terra&Sky','Super Tech','Blackweb','Auto Drive','Way To Celebrate','Brahma','Expert Grill','Adventure Force','Kid Connection','Play Day','Hotel Style','Your Zone','Colorplace','My Life As','Tred Safe','Protâgâ','My Sweet Love','Chapter','Kidde','Walmart Grill','Adventure Wheels','Fun 2 Bake','Faded Glory Leather','Backyard Grill','Mainstays Kids','Russell','Swiss Tech','Danskin Now','Garanimals','Bhg','Fall And Winter Wm','Fetchwear Wm','Value','Fieldpack Marketside','Good N Clean','Rumgria','Bloke','Collini','Cruin','Endroit','Flusso','Fruttuosa','Hinnant','Loin','Salvare','Cher','Volca','Grani Lambrusco','Hinnant','Viamora','Winemakers Selection','Mcclaren Farms') THEN "PB" ELSE "OTHER" END AS brand_breakout
from g left outer join d on g.rpt_dt=d.cal_dt 
left join f on f.mds_fam_id=g.catlg_item_id;
