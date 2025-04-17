-- backup offline subcat lkp table
CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_offline_subcat_mapping_FY26Mar` AS
(SELECT * FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_offline_subcat_mapping_FY26Feb`);

-- backup online subcat lkp table
CREATE OR REPLACE TABLE `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_online_subcat_mapping_FY26Mar` AS
(SELECT * FROM `wmt-mint-mmr-mw-prod.MMR_numerator.lkp_online_subcat_mapping_FY26Feb`);
