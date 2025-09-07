CREATE TABLE ctlg_electroniz.sch_domain_finance_gold.aggregated_store_sales_by_quarter AS
(
  SELECT
    YEAR(order_date) AS year,
    QUARTER(order_date) AS quarter,
    round(sum(sale_price_usd), 2) as aggregated_sales_price
  FROM
    ctlg_electroniz.sch_silver.tbl_silver_orders
  GROUP BY
    YEAR(order_date),
    QUARTER(order_date)
);