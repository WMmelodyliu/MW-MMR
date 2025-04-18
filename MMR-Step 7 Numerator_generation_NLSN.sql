#4a
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.non_d82_sales_reg` AS
SELECT
  wm_year_nbr,
  wm_month_nbr,
  visit_type,
  visit_subtype_code,
  acctg_dept_nbr,
  dept_catg_grp_nbr,
  dept_category_nbr,
  dept_subcatg_nbr,
  dept_excl,
  sub_cat_excl,
  alt_mapping,
  use_nielsen_upc,
  subcat_mmr_hier_id,
  upc_mmr_hier_id,
  override_mmr_hier_id,
  sales,
  department_builder,
  mmr_hier_id AS mmr_hier_id,
  Excluded
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    use_nielsen_upc,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    override_mmr_hier_id,
    sales,
    department_builder,
    mmr_hier_id,
    CASE
      WHEN dept_excl IS TRUE THEN "Y"
      WHEN sub_cat_excl IS TRUE THEN "Y"
      WHEN mmr_hier_id IN ("MMR000000", "MMR010000", "MMR180000", "MMR220000", "MMR440000", "MMR510000", "MMR600000") THEN "Y"
    ELSE
    "N"
  END
    AS Excluded
  FROM (
    SELECT
      wm_year_nbr,
      wm_month_nbr,
      visit_type,
      visit_subtype_code,
      acctg_dept_nbr,
      dept_catg_grp_nbr,
      dept_category_nbr,
      dept_subcatg_nbr,
      dept_excl,
      sub_cat_excl,
      alt_mapping,
      use_nielsen_upc,
      mmr_temp_scan.mmr_hier_id AS subcat_mmr_hier_id,
      r_mmr_hier_id AS upc_mmr_hier_id,
      override_mmr_hier_id,
      sales,
      department_builder,
      CASE
        WHEN override_mmr_hier_id IS NULL OR override_mmr_hier_id="" THEN CASE
        WHEN use_nielsen_upc='Y'
      AND r_mmr_hier_id !="" THEN r_mmr_hier_id
      ELSE
      mmr_temp_scan.mmr_hier_id
    END
      ELSE
      override_mmr_hier_id
    END
      AS mmr_hier_id
    FROM (
      SELECT
        wm_year_nbr AS wm_year_nbr,
        wm_month_nbr AS wm_month_nbr,
        visit_type AS visit_type,
        visit_subtype_code AS visit_subtype_code,
        acctg_dept_nbr AS acctg_dept_nbr,
        dept_catg_grp_nbr AS dept_catg_grp_nbr,
        dept_category_nbr AS dept_category_nbr,
        dept_subcatg_nbr AS dept_subcatg_nbr,
        dept_excl AS dept_excl,
        sub_cat_excl AS sub_cat_excl,
        alt_mapping AS alt_mapping,
        use_nielsen_upc AS use_nielsen_upc,
        mmr_hier_id AS mmr_hier_id,
        r_mmr_hier_id AS r_mmr_hier_id,
        override_mmr_hier_id AS override_mmr_hier_id,
        SUM(total_sales) AS sales,
        mdse_subcatg_nbr AS mdse_subcatg_nbr
      FROM
        `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_scan_FY26Mar_NLSN` mmr_report_temp_scan --update --> step 5 offline NLSN
      WHERE
        acctg_dept_nbr NOT IN (82)
        AND upc_nbr NOT IN (7874235474)
        -- AND wm_year_nbr = 2023
        -- AND wm_month_nbr = 6
      GROUP BY
        wm_year_nbr,
        wm_month_nbr,
        visit_type,
        visit_subtype_code,
        acctg_dept_nbr,
        dept_catg_grp_nbr,
        dept_category_nbr,
        dept_subcatg_nbr,
        dept_excl,
        sub_cat_excl,
        alt_mapping,
        use_nielsen_upc,
        mmr_hier_id,
        r_mmr_hier_id,
        override_mmr_hier_id,
        mdse_subcatg_nbr) mmr_temp_scan
    INNER JOIN (
      SELECT
        *
      FROM
        `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_mmr_hier_id_restatement_FY26Feb_NLSN`) lookup_mmr --update --> lookup table
    ON
      mmr_temp_scan.mmr_hier_id = lookup_mmr.mmr_hier_id))
WHERE
  Excluded!='Y';
  #4b
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.d82_sales_reg` AS
SELECT
  wm_year_nbr,
  wm_month_nbr,
  visit_type,
  visit_subtype_code,
  upc_nbr,
  item_nbr,
  acctg_dept_nbr,
  dept_catg_grp_nbr,
  dept_category_nbr,
  dept_subcatg_nbr,
  dept_excl,
  sub_cat_excl,
  alt_mapping,
  use_nielsen_upc,
  subcat_mmr_hier_id,
  upc_mmr_hier_id,
  override_mmr_hier_id,
  sales,
  mmr_hier_id,
  Excluded
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    upc_nbr,
    item_nbr,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    use_nielsen_upc,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    override_mmr_hier_id,
    sales,
    mmr_hier_id,
    CASE
      WHEN dept_excl IS TRUE THEN "Y"
      WHEN sub_cat_excl IS TRUE THEN "Y"
      WHEN mmr_hier_id IN ("MMR180000", "MMR440000", "MMR510000", "MMR220000") THEN "Y"
    ELSE
    "N"
  END
    AS Excluded
  FROM (
    SELECT
      wm_year_nbr,
      wm_month_nbr,
      visit_type,
      visit_subtype_code,
      upc_nbr,
      item_nbr,
      acctg_dept_nbr,
      dept_catg_grp_nbr,
      dept_category_nbr,
      dept_subcatg_nbr,
      dept_excl,
      sub_cat_excl,
      alt_mapping,
      use_nielsen_upc,
      mmr_hier_id AS subcat_mmr_hier_id,
      r_mmr_hier_id AS upc_mmr_hier_id,
      override_mmr_hier_id,
      sales,
      CASE
        WHEN override_mmr_hier_id IS NULL OR override_mmr_hier_id ="" THEN CASE
        WHEN use_nielsen_upc='Y'
      AND r_mmr_hier_id !="" THEN r_mmr_hier_id
      ELSE
      mmr_hier_id
    END
      ELSE
      override_mmr_hier_id
    END
      AS mmr_hier_id
    FROM (
      SELECT
        wm_year_nbr AS wm_year_nbr,
        wm_month_nbr AS wm_month_nbr,
        visit_type AS visit_type,
        visit_subtype_code AS visit_subtype_code,
        upc_nbr AS upc_nbr,
        item_nbr AS item_nbr,
        acctg_dept_nbr AS acctg_dept_nbr,
        dept_catg_grp_nbr AS dept_catg_grp_nbr,
        dept_category_nbr AS dept_category_nbr,
        dept_subcatg_nbr AS dept_subcatg_nbr,
        dept_excl AS dept_excl,
        sub_cat_excl AS sub_cat_excl,
        alt_mapping AS alt_mapping,
        use_nielsen_upc AS use_nielsen_upc,
        mmr_hier_id AS mmr_hier_id,
        r_mmr_hier_id AS r_mmr_hier_id,
        override_mmr_hier_id AS override_mmr_hier_id,
        SUM(total_sales) AS sales
      FROM
        `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_scan_FY26Mar_NLSN` mmr_report_temp_scan --update
      WHERE
        upc_nbr NOT IN (7874235474)
        AND acctg_dept_nbr IN (82)
        -- AND wm_year_nbr = 2023
        -- AND wm_month_nbr = 6
      GROUP BY
        wm_year_nbr,
        wm_month_nbr,
        visit_type,
        visit_subtype_code,
        upc_nbr,
        item_nbr,
        acctg_dept_nbr,
        dept_catg_grp_nbr,
        dept_category_nbr,
        dept_subcatg_nbr,
        dept_excl,
        sub_cat_excl,
        alt_mapping,
        use_nielsen_upc,
        mmr_hier_id,
        r_mmr_hier_id,
        override_mmr_hier_id)))
WHERE
  Excluded !='Y';
  #4c
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.d82_sales_2_reg` AS -- change
SELECT
  wm_year_nbr,
  wm_month_nbr,
  visit_type,
  visit_subtype_code,
  upc_nbr,
  item_nbr,
  acctg_dept_nbr,
  dept_catg_grp_nbr,
  dept_category_nbr,
  dept_subcatg_nbr,
  dept_excl,
  sub_cat_excl,
  alt_mapping,
  use_nielsen_upc,
  subcat_mmr_hier_id,
  upc_mmr_hier_id,
  override_mmr_hier_id,
  sales,
  Excluded,
  item_excluded,
  CASE
    WHEN mmr_hier_id='MMR070200' THEN 'MMR070100'
  ELSE
  mmr_hier_id
END
  AS mmr_hier_id,
  date_added
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    upc_nbr,
    item_nbr,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    use_nielsen_upc,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    override_mmr_hier_id,
    sales,
    Excluded,
    NULL AS item_excluded,
    mmr_hier_id,
    NULL AS date_added
  FROM
    `wmt-mint-mmr-mw-prod.merch_mw_numerator.d82_sales_reg`
  WHERE
    mmr_hier_id!='Parse'
  -- UNION ALL -- change
  -- SELECT
  --   d82_aggregation.wm_year_nbr,
  --   d82_aggregation.wm_month_nbr,
  --   d82_aggregation.visit_type,
  --   d82_aggregation.visit_subtype_code,
  --   d82_aggregation.upc_nbr,
  --   d82_aggregation.item_nbr,
  --   d82_aggregation.acctg_dept_nbr,
  --   d82_aggregation.dept_catg_grp_nbr,
  --   d82_aggregation.dept_category_nbr,
  --   d82_aggregation.dept_subcatg_nbr,
  --   d82_aggregation.dept_excl,
  --   d82_aggregation.sub_cat_excl,
  --   d82_aggregation.alt_mapping,
  --   d82_aggregation.use_nielsen_upc,
  --   d82_aggregation.subcat_mmr_hier_id,
  --   d82_aggregation.upc_mmr_hier_id,
  --   d82_aggregation.override_mmr_hier_id,
  --   d82_aggregation.sales,
  --   d82_aggregation.Excluded,
  --   d82_lookup.item_excluded,
  --   d82_lookup.NPD_mmr_id AS mmr_hier_id,
  --   --NLSN/NPD ----update
  --   d82_lookup.date_added
  -- FROM (
  --   SELECT
  --     *
  --   FROM
  --     `wmt-mint-mmr-mw-prod.merch_mw_numerator.d82_sales_reg`
  --   WHERE
  --     mmr_hier_id='Parse') d82_aggregation
  -- INNER JOIN (
  --   SELECT
  --     *
  --   FROM
  --     `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_d82_item_mapping_fy24Aug_restatement`) d82_lookup --update
  -- ON
  --   CAST(d82_aggregation.upc_nbr AS string) = CAST(d82_lookup.upc_nbr AS STRING) --change in query
  --   -- CAST(d82_aggregation.upc_nbr AS string) = d82_lookup.upc_nbr --change in query
  --   AND d82_aggregation.item_nbr = d82_lookup.item_nbr 
  -- WHERE
  --   item_excluded IS FALSE
  );

  #4d
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.ogp_sales_reg` AS
SELECT
  wm_year_nbr,
  wm_month_nbr,
  mmr_hier_id,
  SUM(sales) AS OGP
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    Excluded,
    mmr_hier_id,
    sales,
    use_nielsen_upc,
    override_mmr_hier_id,
    department_builder
  FROM
    `wmt-mint-mmr-mw-prod.merch_mw_numerator.non_d82_sales_reg`
  WHERE
    visit_type=86
    OR (visit_type=1
      AND visit_subtype_code IN (190,
        191))
  UNION ALL
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    Excluded,
    mmr_hier_id,
    SUM(sales) AS sale,
    NULL AS use_nielsen_upc,
    NULL AS override_mmr_hier_id,
    NULL AS department_builder
  FROM
    `wmt-mint-mmr-mw-prod.merch_mw_numerator.d82_sales_2_reg`
  WHERE
    visit_type=86
    OR (visit_type=1
      AND visit_subtype_code IN (190,
        191))
  GROUP BY
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    Excluded,
    mmr_hier_id)
WHERE
  (visit_type=86
    AND visit_subtype_code IN (133,
      150,
      197,
      207,
      208,
      220))
  OR (visit_type=1
    AND visit_subtype_code IN (190,
      191))
GROUP BY
  wm_year_nbr,
  wm_month_nbr,
  mmr_hier_id;
  #4e
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_reg` AS
SELECT
  wm_year_nbr,
  wm_month_nbr,
  mmr_hier_id,
  SUM(sales) AS offline_sales
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    Excluded,
    mmr_hier_id,
    sales,
    use_nielsen_upc,
    override_mmr_hier_id,
    department_builder
  FROM
    `wmt-mint-mmr-mw-prod.merch_mw_numerator.non_d82_sales_reg`
  WHERE
    NOT (visit_type=86
      OR (visit_type=1
        AND visit_subtype_code IN (190,
          191)))
  UNION ALL
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    Excluded,
    mmr_hier_id,
    SUM(sales),
    NULL AS use_nielsen_upc,
    NULL AS override_mmr_hier_id,
    NULL AS department_builder
  FROM
    `wmt-mint-mmr-mw-prod.merch_mw_numerator.d82_sales_2_reg`
  WHERE
    NOT (visit_type=86
      OR (visit_type=1
        AND visit_subtype_code IN (190,
          191)))
  GROUP BY
    wm_year_nbr,
    wm_month_nbr,
    visit_type,
    visit_subtype_code,
    acctg_dept_nbr,
    dept_catg_grp_nbr,
    dept_category_nbr,
    dept_subcatg_nbr,
    dept_excl,
    sub_cat_excl,
    alt_mapping,
    subcat_mmr_hier_id,
    upc_mmr_hier_id,
    Excluded,
    mmr_hier_id)
GROUP BY
  wm_year_nbr,
  wm_month_nbr,
  mmr_hier_id;
  #4f
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.5p_sales_reg` AS
SELECT
  wm_year_nbr,
  wm_month_num_of_yr AS wm_month_nbr,
  mmr_hier_id,
  SUM(S2H) AS S2H,
  SUM(S2S) AS S2S,
  SUM(PUT) AS PUT,
  SUM(SFS) AS SFS,
  SUM(MP) AS MP,
  SUM(total_5p_Sales) AS total_5p_Sales
FROM (
  SELECT
    div_id,
    div_desc,
    super_dept_id,
    super_dept_desc,
    dept_id,
    dept_desc,
    categ_id,
    category_desc,
    sub_categ_id,
    subcategory_desc,
    wm_year_nbr,
    wm_month_num_of_yr,
    mmr_hier_id,
    watch,
    sub_cat_excl,
    S2H,
    S2S,
    PUT,
    SFS,
    MP,
    (S2H+S2S+PUT+SFS+MP) AS total_5p_Sales
  FROM (
    SELECT
      div_id,
      div_desc,
      super_dept_id,
      super_dept_desc,
      dept_id,
      dept_desc,
      categ_id,
      category_desc,
      sub_categ_id,
      subcategory_desc,
      wm_year_nbr,
      wm_month_num_of_yr,
      mmr_hier_id,
      watch,
      sub_cat_excl,
      CASE
        WHEN S2H IS NULL THEN 0
      ELSE
      S2H
    END
      AS S2H,
      CASE
        WHEN S2S IS NULL THEN 0
      ELSE
      S2S
    END
      AS S2S,
      CASE
        WHEN PUT IS NULL THEN 0
      ELSE
      PUT
    END
      AS PUT,
      CASE
        WHEN SFS IS NULL THEN 0
      ELSE
      SFS
    END
      AS SFS,
      CASE
        WHEN MP IS NULL THEN 0
      ELSE
      MP
    END
      AS MP
    FROM (
      SELECT
        div_id AS div_id,
        div_desc AS div_desc,
        super_dept_id AS super_dept_id,
        super_dept_desc AS super_dept_desc,
        dept_id AS dept_id,
        dept_desc AS dept_desc,
        categ_id AS categ_id,
        category_desc AS category_desc,
        sub_categ_id AS sub_categ_id,
        subcategory_desc AS subcategory_desc,
        wm_year_nbr AS wm_year_nbr,
        wm_month_num_of_yr AS wm_month_num_of_yr,
        mmr_hier_id AS mmr_hier_id,
        watch AS watch,
        sub_cat_excl AS sub_cat_excl,
        SUM(s2h_sales_net) AS S2H,
        SUM(s2s_sales_net) AS S2S,
        SUM(put_sales_net) AS PUT,
        SUM(sfs_sales_net) AS SFS,
        SUM(mp_sales_net) AS MP
      FROM
        `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_5p_sales_FY26Mar_NLSN` mmr_report_temp_5p_sales --update
      WHERE
        watch = FALSE
        -- AND wm_year_nbr = "2023"
        -- AND wm_month_num_of_yr = 6
      GROUP BY
        div_id,
        div_desc,
        super_dept_id,
        super_dept_desc,
        dept_id,
        dept_desc,
        categ_id,
        category_desc,
        sub_categ_id,
        subcategory_desc,
        wm_year_nbr,
        wm_month_num_of_yr,
        mmr_hier_id,
        watch,
        sub_cat_excl))
  WHERE
    sub_cat_excl IS FALSE)
GROUP BY
  wm_year_nbr,
  wm_month_num_of_yr,
  mmr_hier_id;
  #4g
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.pt_sales_reg` AS
SELECT
  wm_year_nbr,
  wm_month_num_of_yr AS wm_month_nbr,
  mmr_hier_id,
  SUM(S2H) AS S2H,
  SUM(S2S) AS S2S,
  SUM(PUT) AS PUT,
  SUM(SFS) AS SFS,
  SUM(MP) AS MP,
  SUM(total_5p_Sales) AS total_5p_Sales
FROM (
  SELECT
    prod_type_lookup.prod_type_nm,
    prod_type_nm_excl,
    prod_type_lookup.watch,
    NLSN_mmr_id, --NLSN/NPD --update
    catlg_item_id,
    div_id,
    div_desc,
    super_dept_id,
    super_dept_desc,
    dept_id,
    dept_desc,
    categ_id,
    category_desc,
    sub_categ_id,
    subcategory_desc,
    wm_year_nbr,
    wm_month_num_of_yr,
    subcat_mmr_hier_id,
    subcat_watch,
    sub_cat_excl,
    upc,
    wupc,
    prod_nm,
    S2H,
    S2S,
    PUT,
    SFS,
    MP,
    gtin,
    total_5p_sales,
    NLSN_mmr_id AS mmr_hier_id --NLSN/NPD --update
  FROM (
    SELECT
      REPLACE(TRIM(prod_type_nm), '"', "") AS prod_type_nm,
      prod_type_nm_excl,
      Watch,
      NLSN_mmr_id,--NLSN/NPD --update
      WM_Date_Added,
      mmr_rstmt_cycle
    FROM
      `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_producttype_FY26Mar`) prod_type_lookup --update
  JOIN (
    SELECT
      catlg_item_id,
      div_id,
      div_desc,
      super_dept_id,
      super_dept_desc,
      dept_id,
      dept_desc,
      categ_id,
      category_desc,
      sub_categ_id,
      subcategory_desc,
      wm_year_nbr,
      wm_month_num_of_yr,
      subcat_mmr_hier_id,
      subcat_watch,
      sub_cat_excl,
      upc,
      wupc,
      prod_nm,
      TRIM(prod_type_nm) AS prod_type_nm,
      S2H,
      S2S,
      PUT,
      SFS,
      MP,
      (S2H+S2S+PUT+SFS+MP) AS total_5p_sales,
      gtin
    FROM (
      SELECT
        catlg_item_id,
        div_id,
        div_desc,
        super_dept_id,
        super_dept_desc,
        dept_id,
        dept_desc,
        categ_id,
        category_desc,
        sub_categ_id,
        subcategory_desc,
        wm_year_nbr,
        wm_month_num_of_yr,
        subcat_mmr_hier_id,
        subcat_watch,
        sub_cat_excl,
        upc,
        wupc,
        prod_nm,
        prod_type_nm,
        CASE
          WHEN S2H IS NULL THEN 0
        ELSE
        S2H
      END
        AS S2H,
        CASE
          WHEN S2S IS NULL THEN 0
        ELSE
        S2S
      END
        AS S2S,
        CASE
          WHEN PUT IS NULL THEN 0
        ELSE
        PUT
      END
        AS PUT,
        CASE
          WHEN SFS IS NULL THEN 0
        ELSE
        SFS
      END
        AS SFS,
        CASE
          WHEN MP IS NULL THEN 0
        ELSE
        MP
      END
        AS MP,
        gtin
      FROM (
        SELECT
          catlg_item_id AS catlg_item_id,
          div_id AS div_id,
          div_desc AS div_desc,
          super_dept_id AS super_dept_id,
          super_dept_desc AS super_dept_desc,
          dept_id AS dept_id,
          dept_desc AS dept_desc,
          categ_id AS categ_id,
          category_desc AS category_desc,
          sub_categ_id AS sub_categ_id,
          subcategory_desc AS subcategory_desc,
          wm_year_nbr AS wm_year_nbr,
          wm_month_num_of_yr AS wm_month_num_of_yr,
          mmr_hier_id AS subcat_mmr_hier_id,
          watch AS subcat_watch,
          sub_cat_excl AS sub_cat_excl,
          upc AS upc,
          wupc AS wupc,
          prod_nm AS prod_nm,
          prod_type_nm AS prod_type_nm,
          SUM(s2h_sales_net) AS S2H,
          SUM(s2s_sales_net) AS S2S,
          SUM(put_sales_net) AS PUT,
          SUM(sfs_sales_net) AS SFS,
          SUM(mp_sales_net) AS MP,
          gtin AS gtin
        FROM
          `wmt-mint-mmr-mw-prod.MMR_numerator.mmr2_temp_5p_sales_FY26Mar_NLSN` mmr_report_temp_5p_sales --update
        WHERE
          watch = TRUE
          -- AND wm_year_nbr = "2023"
          -- AND wm_month_num_of_yr = 6
        GROUP BY
          catlg_item_id,
          div_id,
          div_desc,
          super_dept_id,
          super_dept_desc,
          dept_id,
          dept_desc,
          categ_id,
          category_desc,
          sub_categ_id,
          subcategory_desc,
          wm_year_nbr,
          wm_month_num_of_yr,
          mmr_hier_id,
          watch,
          sub_cat_excl,
          upc,
          wupc,
          prod_nm,
          prod_type_nm,
          gtin))) mmr_5p_sales
  ON
    prod_type_lookup.prod_type_nm = mmr_5p_sales.prod_type_nm
  WHERE
    prod_type_nm_excl IS FALSE)
GROUP BY
  wm_year_nbr,
  wm_month_num_of_yr,
  mmr_hier_id;
  #4h
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.ogp_5p_sales_reg` AS
SELECT
  wm_year_nbr,
  wm_month_nbr,
  mmr_hier_id,
  SUM(online_sales) AS online_sales
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    mmr_hier_id,
    S2H,
    S2S,
    PUT,
    SFS,
    MP,
    total_5p_Sales,
    OGP,
    (total_5p_Sales+OGP) AS online_sales
  FROM (
    SELECT
      wm_year_nbr,
      wm_month_nbr,
      mmr_hier_id,
      CASE
        WHEN S2H IS NULL THEN 0
      ELSE
      S2H
    END
      AS S2H,
      CASE
        WHEN S2S IS NULL THEN 0
      ELSE
      S2S
    END
      AS S2S,
      CASE
        WHEN PUT IS NULL THEN 0
      ELSE
      PUT
    END
      AS PUT,
      CASE
        WHEN SFS IS NULL THEN 0
      ELSE
      SFS
    END
      AS SFS,
      CASE
        WHEN MP IS NULL THEN 0
      ELSE
      MP
    END
      AS MP,
      CASE
        WHEN total_5p_Sales IS NULL THEN 0
      ELSE
      total_5p_Sales
    END
      AS total_5p_Sales,
      CASE
        WHEN OGP IS NULL THEN 0
      ELSE
      OGP
    END
      AS OGP
    FROM (
      SELECT
        CAST(wm_year_nbr AS int64) AS wm_year_nbr,
        wm_month_nbr,
        mmr_hier_id,
        S2H,
        S2S,
        PUT,
        SFS,
        MP,
        total_5p_Sales,
        NULL AS OGP
      FROM
        `wmt-mint-mmr-mw-prod.merch_mw_numerator.5p_sales_reg`
      UNION ALL
      SELECT
        CAST(wm_year_nbr AS int64) AS wm_year_nbr,
        wm_month_nbr,
        mmr_hier_id,
        S2H,
        S2S,
        PUT,
        SFS,
        MP,
        total_5p_Sales,
        NULL AS OGP
      FROM
        `wmt-mint-mmr-mw-prod.merch_mw_numerator.pt_sales_reg`
      UNION ALL
      SELECT
        wm_year_nbr,
        wm_month_nbr,
        mmr_hier_id AS mmr_hier_id,
        NULL AS S2H,
        NULL AS S2S,
        NULL AS PUT,
        NULL AS SFS,
        NULL AS MP,
        NULL AS total_5p_Sales,
        OGP
      FROM
        `wmt-mint-mmr-mw-prod.merch_mw_numerator.ogp_sales_reg` )))
GROUP BY
  wm_year_nbr,
  wm_month_nbr,
  mmr_hier_id;
  #4i
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_with_mmr_reg` AS
SELECT
  wm_year_nbr,
  mmr_dept_id,
  mmr_category_group_id,
  mmr_category_id,
  mmr_hier_id,
  department_builder,
  reporting_level,
 MMR_SBU as business_unit,  --'MMR_SBU ?'
  MMR_MAJOR_BUSINESS as major_business,--'MMR_MAJOR_BUSINESS ?'

  MMR_DEPT as department,
MMR_CATEGORY_GROUP as category_group,
MMR_CATEGORY as category,
  mmr_hier_concat,
  loopup_mmr.wm_month_nbr,
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr
  FROM
    `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_reg`
  GROUP BY
    wm_year_nbr,
    wm_month_nbr) offline_sales
JOIN (
  SELECT
    *
  FROM
    `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_mmr_hier_id_restatement_FY26Feb_NLSN` --update
  CROSS JOIN (
    SELECT
      *
    FROM
      UNNEST ([1,2,3,4,5,6,7,8,9,10,11,12]) AS wm_month_nbr)) loopup_mmr
ON
  offline_sales.wm_month_nbr=loopup_mmr.wm_month_nbr;
  #4j
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_final_reg` AS
SELECT
  wm_year_nbr,
  wm_month_nbr,
  mmr_dept_id,
  mmr_category_group_id,
  mmr_category_id,
  business_unit,
  major_business,
  department,
  category_group,
  category,
  mmr_hier_id,
  reporting_level,
  CASE
    WHEN offline_sales<0 THEN 0
  ELSE
  offline_sales
END
  AS offline_sales,
  CASE
    WHEN wm_month_nbr=1 THEN "February"
    WHEN wm_month_nbr=2 THEN "March"
    WHEN wm_month_nbr=3 THEN "April"
    WHEN wm_month_nbr=4 THEN "May"
    WHEN wm_month_nbr=5 THEN "June"
    WHEN wm_month_nbr=6 THEN "July"
    WHEN wm_month_nbr=7 THEN "August"
    WHEN wm_month_nbr=8 THEN "September"
    WHEN wm_month_nbr=9 THEN "October"
    WHEN wm_month_nbr=10 THEN "November"
    WHEN wm_month_nbr=11 THEN "December"
    WHEN wm_month_nbr=12 THEN "January"
END
  AS wm_month_desc
FROM (
  SELECT
    wm_year_nbr,
    wm_month_nbr,
    mmr_dept_id,
    mmr_category_group_id,
    mmr_category_id,
    reporting_level,
    business_unit,
    major_business,
    department,
    category_group,
    category,
    mmr_hier_id,
    SUM(offline_sales) AS offline_sales
  FROM (
    SELECT
      offline_sales_with_mmr.wm_year_nbr,
      mmr_dept_id,
      mmr_category_group_id,
      mmr_category_id,
      offline_sales_with_mmr.mmr_hier_id, --CHANGED
      department_builder,
      reporting_level,
      business_unit,
      major_business,
      department,
      category_group,
      category,
      mmr_hier_concat,
      offline_sales_with_mmr.wm_month_nbr,
      NULL AS offline_sales
    FROM (
      SELECT
        *
      FROM
        `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_reg`)offline_sales
    RIGHT JOIN ( 
      SELECT
        *
      FROM
        `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_with_mmr_reg`) offline_sales_with_mmr
    ON
      offline_sales.mmr_hier_id=offline_sales_with_mmr.mmr_hier_id
      AND offline_sales.wm_month_nbr=offline_sales_with_mmr.wm_month_nbr
      AND offline_sales.wm_year_nbr=offline_sales_with_mmr.wm_year_nbr
    WHERE
      offline_sales.mmr_hier_id IS NULL
      AND offline_sales.wm_month_nbr IS NULL
      AND offline_sales.wm_year_nbr IS NULL
      AND reporting_level!='NOT_REPORTED')
  GROUP BY
    wm_year_nbr,
    wm_month_nbr,
    mmr_dept_id,
    mmr_category_group_id,
    mmr_category_id,
    reporting_level,
    business_unit,
    major_business,
    department,
    category_group,
    category,
    mmr_hier_id
  UNION ALL
  SELECT
    offline_sales_tbl.wm_year_nbr,
    offline_sales_tbl.wm_month_nbr,
    mmr_dept_id,
    mmr_category_group_id,
    mmr_category_id,
    reporting_level,
    business_unit,
    major_business,
    department,
    category_group,
    category,
    IFNULL(offline_sales_tbl.mmr_hier_id,offline_sales_with_mmr.mmr_hier_id) mmr_hier_id, --> CHECK (offline_sales_tbl.mmr_hier_id)
    SUM(offline_sales) AS offline_sales
  FROM (
    SELECT
      *
    FROM
      `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_reg`)offline_sales_tbl
  JOIN ( --> JOIN CHECK
    SELECT
      *
    FROM
      `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_with_mmr_reg`) offline_sales_with_mmr
  ON
    offline_sales_tbl.mmr_hier_id=offline_sales_with_mmr.mmr_hier_id
    AND offline_sales_tbl.wm_month_nbr=offline_sales_with_mmr.wm_month_nbr
    AND offline_sales_tbl.wm_year_nbr=offline_sales_with_mmr.wm_year_nbr
  GROUP BY
    wm_year_nbr,
    wm_month_nbr,
    mmr_dept_id,
    mmr_category_group_id,
    mmr_category_id,
    reporting_level,
    business_unit,
    major_business,
    department,
    category_group,
    category,
    mmr_hier_id)
ORDER BY
  wm_year_nbr ASC,
  wm_month_nbr ASC,
  business_unit ASC,
  major_business ASC,
  department ASC,
  category_group ASC,
  category ASC;
  #4k
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.online_sales_with_mmr_reg` AS
SELECT
  mmr_dept_id,
  mmr_category_group_id,
  mmr_category_id,
  department_builder,
  reporting_level,
   MMR_SBU as business_unit,  --'MMR_SBU ?'
  MMR_MAJOR_BUSINESS as major_business,--'MMR_MAJOR_BUSINESS ?'

  MMR_DEPT as department,
MMR_CATEGORY_GROUP as category_group,
MMR_CATEGORY as category,
  mmr_hier_concat,
  wm_year_nbr,
  wm_month_nbr,
  p5_ogp.mmr_hier_id,
  CASE
    WHEN online_sales<0 THEN 0
  ELSE
  online_sales
END
  AS online_sales,
  CASE
    WHEN wm_month_nbr=1 THEN "February"
    WHEN wm_month_nbr=2 THEN "March"
    WHEN wm_month_nbr=3 THEN "April"
    WHEN wm_month_nbr=4 THEN "May"
    WHEN wm_month_nbr=5 THEN "June"
    WHEN wm_month_nbr=6 THEN "July"
    WHEN wm_month_nbr=7 THEN "August"
    WHEN wm_month_nbr=8 THEN "September"
    WHEN wm_month_nbr=9 THEN "October"
    WHEN wm_month_nbr=10 THEN "November"
    WHEN wm_month_nbr=11 THEN "December"
    WHEN wm_month_nbr=12 THEN "January"
END
  AS wm_month_desc
FROM (
  SELECT
    *
  FROM
    `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_mmr_hier_id_restatement_FY26Feb_NLSN`)lookup_mmr --update
JOIN (
  SELECT
    *
  FROM
    `wmt-mint-mmr-mw-prod.merch_mw_numerator.ogp_5p_sales_reg`)p5_ogp
ON
  lookup_mmr.mmr_hier_id=p5_ogp.mmr_hier_id;
  #4l
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.online_sales_final_reg` AS
SELECT
  *
FROM (
  SELECT
    *
  FROM (
    SELECT
      wm_year_nbr,
      wm_month_nbr,
      wm_month_desc,
      mmr_dept_id,
      business_unit,
      major_business,
      department,
      mmr_hier_id,
      reporting_level,
      SUM(online_sales) AS online_sales,
      NULL AS mmr_category_group_id,
      NULL AS mmr_category_id,
      NULL AS category_group,
      NULL AS category
    FROM (
      SELECT
        mmr_dept_id,
        mmr_category_group_id,
        mmr_category_id,
        department_builder,
        CASE
          WHEN reporting_level!="DEPARTMENT" THEN "DEPARTMENT_TOTAL"
        ELSE
        reporting_level
      END
        AS reporting_level,
        business_unit,
        major_business,
        department,
        category_group,
        category,
        mmr_hier_concat,
        wm_year_nbr,
        wm_month_nbr,
        mmr_hier_id_orig,
        online_sales,
        wm_month_desc,
        RPAD(SUBSTR(mmr_hier_id_orig, 0, 5), LENGTH(mmr_hier_id_orig), "0") AS mmr_hier_id
      FROM (
        SELECT
          mmr_dept_id,
          mmr_category_group_id,
          mmr_category_id,
          department_builder,
          reporting_level,
          business_unit,
          major_business,
          department,
          category_group,
          category,
          mmr_hier_concat,
          wm_year_nbr,
          wm_month_nbr,
          mmr_hier_id AS mmr_hier_id_orig,
          online_sales,
          wm_month_desc
        FROM
          `wmt-mint-mmr-mw-prod.merch_mw_numerator.online_sales_with_mmr_reg`
        WHERE
          mmr_dept_id NOT IN ('3_07',
            '3_16',
            '3_34',
            '3_52',
            '3_53',
            '3_54',
            '3_02',
            '3_29',
            '3_03',
            '3_39',
            '3_48')))
    GROUP BY
      wm_year_nbr,
      wm_month_nbr,
      wm_month_desc,
      mmr_dept_id,
      business_unit,
      major_business,
      department,
      mmr_hier_id,
      reporting_level
    UNION ALL
    SELECT
      wm_year_nbr,
      wm_month_nbr,
      wm_month_desc,
      mmr_dept_id,
      business_unit,
      major_business,
      department,
      mmr_hier_id,
      reporting_level,
      SUM(online_sales) AS online_sales,
      mmr_category_group_id,
      mmr_category_id,
      category_group,
      category
    FROM (
      SELECT
        *
      FROM
        `wmt-mint-mmr-mw-prod.merch_mw_numerator.online_sales_with_mmr_reg`
      WHERE
        mmr_dept_id IN ('3_07',
          '3_16',
          '3_34',
          '3_52',
          '3_53',
          '3_54',
          '3_02',
          '3_29',
          '3_03',
          '3_39',
          '3_48'))
    GROUP BY
      wm_year_nbr,
      wm_month_nbr,
      wm_month_desc,
      mmr_dept_id,
      mmr_category_group_id,
      mmr_category_id,
      business_unit,
      major_business,
      department,
      category_group,
      category,
      mmr_hier_id,
      reporting_level))
ORDER BY
  wm_year_nbr ASC,
  wm_month_nbr ASC,
  business_unit ASC,
  major_business ASC,
  department ASC,
  category_group ASC,
  category ASC;
  #4m
CREATE OR REPLACE TABLE
  `wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn` AS -- update
SELECT
  fiscal_year,
  wm_year_nbr,
  wm_month_nbr,
  wm_month_desc,
  cal_year_nbr,
  cal_month_nbr,
  PARSE_DATE('%m/%d/%Y',  date )AS date,
  Business_Unit,
  Major_Business,
  Department,
  CASE
    WHEN Department IN ("Cameras and Supplies", "Kitchen", "Electronics", "Toys", "Stationery") AND Channel="Total" THEN Category_Group
    WHEN reporting_level="DEPARTMENT_TOTAL"
  AND Channel IN ("Total",
    "Online") THEN CONCAT(Department, " - Online")
    WHEN reporting_level="DEPARTMENT" THEN Department
  ELSE
  Category_Group
END
  AS Category_Group,
  CASE
    WHEN Department IN ("Cameras and Supplies", "Kitchen", "Electronics", "Toys", "Stationery") AND Channel="Total" THEN Category_Group
    WHEN reporting_level="DEPARTMENT_TOTAL"
  AND Channel IN ("Total",
    "Online") THEN CONCAT(Department, " - Online")
    WHEN reporting_level="CATEGORY_GROUP" THEN Category_Group
    WHEN reporting_level="DEPARTMENT" THEN Department
  ELSE
  Category
END
  AS Category,
  mmr_hier_id,
  reporting_level,
  -- fy_file,
  Channel,
  CAST(WM_Sales AS NUMERIC) AS WM_Sales
FROM (
  SELECT
    fiscal_year,
    wm_year_nbr,
    wm_month_nbr,
    wm_month_desc,
    cal_year_nbr,
    cal_month_nbr,
    date,
    Business_Unit,
    Major_Business,
    Department,
    Category_Group,
    Category,
    mmr_hier_id,
    reporting_level,
    fy_file,
    CASE
      WHEN LENGTH(Channel)=16 THEN SUBSTR(Channel, 4, 7)
      WHEN LENGTH(Channel)=15 THEN SUBSTR(Channel, 4, 6)
    ELSE
    SUBSTR(Channel, 4, 5)
  END
    AS Channel,
    WM_Sales
  FROM (
    SELECT
      fiscal_year,
      wm_year_nbr,
      wm_month_nbr,
      wm_month_desc,
      cal_year_nbr,
      cal_month_nbr,
      date,
      Business_Unit,
      Major_Business,
      Department,
      Category_Group,
      Category,
      mmr_hier_id,
      reporting_level,
      fy_file,
      Channel,
      WM_Sales
    FROM (
      SELECT
        fiscal_year,
        wm_year_nbr,
        wm_month_nbr,
        wm_month_desc,
        cal_year_nbr,
        cal_month_nbr,
        date,
        Business_Unit,
        Major_Business,
        Department,
        Category_Group,
        Category,
        mmr_hier_id,
        reporting_level,
        fy_file,
        name AS Channel,
        value AS WM_Sales
      FROM (
        SELECT
          wm_year_nbr,
          wm_month_nbr,
          mmr_dept_id,
          mmr_category_group_id,
          mmr_category_id,
          business_unit AS Business_Unit,
          major_business AS Major_Business,
          department AS DEPARTMENT,
          category_group AS Category_Group,
          category AS Category,
          online_sales AS WM_Online_Sales,
          offline_sales AS WM_Offline_Sales,
          wm_month_desc,
          mmr_hier_id,
          reporting_level,
          wm_total_sales,
          cal_year_nbr,
          cal_month_nbr,
          fiscal_year,
          CONCAT(CAST(cal_month_nbr AS STRING), "/1/", CAST(cal_year_nbr AS STRING)) AS date,
          fiscal_year AS fy_file
        FROM (
          SELECT
            wm_year_nbr,
            wm_month_nbr,
            mmr_dept_id,
            mmr_category_group_id,
            mmr_category_id,
            business_unit,
            major_business,
            department,
            category_group,
            category,
            online_sales,
            offline_sales,
            wm_month_desc,
            mmr_hier_id,
            reporting_level,
            wm_total_sales,
            cal_year_nbr,
            cal_month_nbr,
            CONCAT("FY ", SUBSTR(CAST(fiscal_year AS STRING), 3, 4)) AS fiscal_year
          FROM (
            SELECT
              offline_final_Sales.wm_year_nbr,
              offline_final_Sales.wm_month_nbr,
              offline_final_Sales.mmr_dept_id,
              offline_final_Sales.mmr_category_group_id,
              offline_final_Sales.mmr_category_id,
              offline_final_Sales.business_unit,
              offline_final_Sales.major_business,
              offline_final_Sales.department,
              offline_final_Sales.category_group,
              offline_final_Sales.category,
              online_sales,
              offline_sales,
              offline_final_Sales.wm_month_desc,
              IFNULL (offline_final_Sales.mmr_hier_id,online_final_Sales.mmr_hier_id) mmr_hier_id, --> CHECK (offline_final_Sales.mmr_hier_id)
              offline_final_Sales.reporting_level,
              CASE
                WHEN offline_sales IS NULL THEN online_sales
                WHEN online_sales IS NULL THEN offline_sales
              ELSE
              online_sales+offline_sales
            END
              AS wm_total_sales,
              CASE
                WHEN offline_final_Sales.wm_month_nbr=12 THEN offline_final_Sales.wm_year_nbr+1
              ELSE
              offline_final_Sales.wm_year_nbr
            END
              AS cal_year_nbr,
              CASE
                WHEN offline_final_Sales.wm_month_nbr=12 THEN 1
              ELSE
              offline_final_Sales.wm_month_nbr+1
            END
              AS cal_month_nbr,
              offline_final_Sales.wm_year_nbr+1 AS fiscal_year
            FROM (
              SELECT
                *
              FROM
                `wmt-mint-mmr-mw-prod.merch_mw_numerator.offline_sales_final_reg`)offline_final_Sales
            FULL JOIN (
              SELECT
                *
              FROM
                `wmt-mint-mmr-mw-prod.merch_mw_numerator.online_sales_final_reg`) online_final_Sales
            ON
              offline_final_Sales.wm_year_nbr=online_final_Sales.wm_year_nbr
              AND offline_final_Sales.wm_month_nbr=online_final_Sales.wm_month_nbr
              AND offline_final_Sales.mmr_hier_id=online_final_Sales.mmr_hier_id))
        WHERE
          wm_month_nbr>0
          AND wm_month_nbr<=12
        ORDER BY
          wm_year_nbr ASC,
          wm_month_nbr ASC,
          business_unit ASC,
          major_business ASC,
          department ASC,
          mmr_hier_id ASC) UNPIVOT INCLUDE NULLS(value FOR name IN (WM_Online_Sales,
            WM_Offline_Sales,
            WM_Total_Sales)))
    WHERE
      WM_Sales IS NOT NULL
      AND reporting_level!='NOT_REPORTED'
));


---NLSN
CREATE OR REPLACE TABLE wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn2 AS --update
SELECT *
FROM wmt-mint-mmr-mw-prod.merch_mw_numerator.FY26Mar_mmr_numerator_bq_nlsn --update
WHERE Business_Unit!= 'GENERAL MERCHANDISE' OR 
      (Business_Unit = 'GENERAL MERCHANDISE' AND Department IN ('CELEBRATION', 'SEASONAL', 'PIECE GOODS AND CRAFTS'));
