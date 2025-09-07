CREATE TABLE ctlg_electroniz.sch_domain_marketing_gold.webhits_by_country AS
(
  SELECT
    country_name,
    count(*) AS hits
  FROM
    ctlg_electroniz.sch_silver.tbl_silver_iplocation
  GROUP BY
    country_name
  HAVING
    count(*) > 1000
);