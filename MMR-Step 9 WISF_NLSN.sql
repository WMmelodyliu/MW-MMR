
CREATE or replace TABLE `wmt-mint-mmr-mw-prod.merch_mw_numerator.agg_sales_FY26Mar_nlsn_reg` AS -- UPDATE NLSN/NPD

SELECT wm_year_nbr,
       wm_month_nbr,
       mmr_hier_id,
       sum(S2H) AS S2H,
       sum(S2S) AS S2S,
       sum(PUT) AS PUT,
       sum(SFS) AS SFS,
       sum(MP) AS MP,
       sum(OGP) AS OGP,
       sum(online_sales_1) AS online_sales_1,
       sum(online_sales_2) AS online_sales_2,
       sum(offline_sales) as offline_sales
FROM
    (SELECT wm_year_nbr,
            wm_month_nbr,
            mmr_hier_id,
            S2H,
            S2S,
            PUT,
            SFS,
            MP,
            total_5p_Sales,
            OGP,
            (total_5p_Sales+OGP) AS online_sales_1,
            (total_5p_Sales-PUT-SFS) AS online_sales_2,
            (OGP+PUT+SFS) AS offline_sales
     FROM
         (SELECT wm_year_nbr,
                 wm_month_nbr,
                 mmr_hier_id,
                 CASE
                     WHEN S2H IS NULL THEN 0
                     ELSE S2H
                     END AS S2H,
                 CASE
                     WHEN S2S IS NULL THEN 0
                     ELSE S2S
                     END AS S2S,
                 CASE
                     WHEN PUT IS NULL THEN 0
                     ELSE PUT
                     END AS PUT,
                 CASE
                     WHEN SFS IS NULL THEN 0
                     ELSE SFS
                     END AS SFS,
                 CASE
                     WHEN MP IS NULL THEN 0
                     ELSE MP
                     END AS MP,
                 CASE
                     WHEN total_5p_Sales IS NULL THEN 0
                     ELSE total_5p_Sales
                     END AS total_5p_Sales,
                 CASE
                     WHEN OGP IS NULL THEN 0
                     ELSE OGP
                     END AS OGP
          FROM
              (SELECT cast(wm_year_nbr AS int64) AS wm_year_nbr,
                      wm_month_nbr,
                      mmr_hier_id,
                      S2H,
                      S2S,
                      PUT,
                      SFS,
                      MP,
                      total_5p_Sales,
                      NULL AS OGP
               FROM `wmt-mint-mmr-mw-prod.merch_mw_numerator.5p_sales_reg` 
               UNION ALL SELECT cast(wm_year_nbr AS int64) AS wm_year_nbr,
                                wm_month_nbr,
                                mmr_hier_id,
                                S2H,
                                S2S,
                                PUT,
                                SFS,
                                MP,
                                total_5p_Sales,
                                NULL AS OGP
               FROM `wmt-mint-mmr-mw-prod.merch_mw_numerator.pt_sales_reg` 
               UNION ALL SELECT wm_year_nbr,
                                wm_month_nbr,
                                mmr_hier_id AS mmr_hier_id,
                                NULL AS S2H,
                                NULL AS S2S,
                                NULL AS PUT,
                                NULL AS SFS,
                                NULL AS MP,
                                NULL AS total_5p_Sales,
                                online_sales as OGP
               FROM `wmt-mint-mmr-mw-prod.merch_mw_numerator.ogp_5p_sales_reg`))) 
GROUP BY wm_year_nbr,
         wm_month_nbr,
         mmr_hier_id;



CREATE or replace TABLE  `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_wisf_FY26Mar_nlsn_reg` AS -- UPDATE NLSN/NPD
WITH cte AS (SELECT wm_year_nbr,
       wm_month_nbr,
       mmr_dept_id,
       mmr_category_group_id,
       mmr_category_id,
       MMR_SBU as business_unit,
MMR_MAJOR_BUSINESS as major_business,
MMR_DEPT as department,
MMR_CATEGORY_GROUP as category_group,
MMR_CATEGORY as category,

       a.mmr_hier_id,
       reporting_level,
       S2H AS S2H,
       S2S AS S2S,
       PUT AS PUT,
       SFS AS SFS,
       MP AS MP,
       OGP AS OGP, CASE
           WHEN wm_month_nbr=1 THEN "February"
           WHEN wm_month_nbr=2 THEN "March"
           WHEN wm_month_nbr=3 THEN "April"
           WHEN wm_month_nbr=4 THEN "May"
           WHEN wm_month_nbr=5 THEN "June"
           WHEN wm_month_nbr=6 THEN "July"
           WHEN wm_month_nbr=7 THEN "August"
           WHEN wm_month_nbr=8 THEN "Octember"
           WHEN wm_month_nbr=9 THEN "October"
           WHEN wm_month_nbr=10 THEN "November"
           WHEN wm_month_nbr=11 THEN "December"
           WHEN wm_month_nbr=12 THEN "January"
           ELSE "Unknown"
           END AS wm_month_desc
FROM `wmt-mint-mmr-mw-prod.merch_mw_numerator.agg_sales_FY26Mar_nlsn_reg` as a --UPDATE NLSN/NPD
INNER JOIN `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_mmr_hier_id_restatement_FY26Feb_NLSN` as b --UPDATE NLSN/NPD
ON a.mmr_hier_id = b.mmr_hier_id
WHERE reporting_level != "NOT_REPORTED"),
cte2 AS (
SELECT wm_year_nbr,
       wm_month_nbr,
       wm_month_desc,
       business_unit,
       major_business,
       department,
       IFNULL(category_group,"") as category_group,
       IFNULL(category, "") as category,
       mmr_hier_id,
       sum(PUT) AS PUT,
       sum(SFS) AS SFS,
       sum(OGP) AS OGP
FROM cte
WHERE mmr_dept_id IN ('3_07','3_16','3_34','3_53','3_54')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
UNION ALL
SELECT wm_year_nbr,
       wm_month_nbr,
       wm_month_desc,
       business_unit,
       major_business,
       department,
       "" AS category_group,
       "" AS category,
       RPAD(LEFT(mmr_hier_id, 5), 9, "0") as mmr_hier_id,
       sum(PUT) AS PUT,
       sum(SFS) AS SFS,
       sum(OGP) AS OGP
FROM cte
WHERE mmr_dept_id NOT IN ('3_07','3_16','3_34','3_53','3_54')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9),
offline AS (SELECT a.fiscal_year,
       a.wm_year_nbr,
       a.wm_month_nbr,
       a.wm_month_desc,
       a.business_unit,
       a.major_business,
       a.department,
       a.category_group,
       a.category,
       a.mmr_hier_id,
       a.wm_sales as offline_sales,
       b.PUT,
       b.SFS,
       b.OGP
FROM (SELECT *
FROM `wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn` --UPDATE NLSN/NPD
WHERE channel = "Offline") a
LEFT JOIN cte2 as b
ON a.wm_year_nbr = b.wm_year_nbr
AND a.wm_month_nbr = b.wm_month_nbr
AND a.mmr_hier_id = b.mmr_hier_id),
online AS (
SELECT c.fiscal_year,
       c.wm_year_nbr,
       c.wm_month_nbr,
       c.wm_month_desc,
       c.business_unit,
       c.major_business,
       c.department,
       c.category_group,
       c.category,
       c.mmr_hier_id,
       0 as offline_sales,
       d.PUT,
       d.SFS,
       d.OGP
FROM (SELECT *
FROM cte2
EXCEPT DISTINCT
SELECT b.*
FROM (SELECT *
FROM `wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn` -- UPDATE NLSN/NPD
WHERE channel = "Offline") a
LEFT JOIN cte2 as b
ON a.wm_year_nbr = b.wm_year_nbr
AND a.wm_month_nbr = b.wm_month_nbr
AND a.mmr_hier_id = b.mmr_hier_id) AS d
INNER JOIN (SELECT *
FROM `wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn` -- UPDATE NLSN/NPD
WHERE channel = "Online") AS c
ON c.wm_year_nbr = d.wm_year_nbr
AND c.wm_month_nbr = d.wm_month_nbr
AND c.mmr_hier_id = d.mmr_hier_id
)
SELECT *
FROM offline
UNION ALL
SELECT *
FROM online
ORDER BY wm_year_nbr, wm_month_nbr, mmr_hier_id;


