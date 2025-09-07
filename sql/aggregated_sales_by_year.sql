CREATE TABLE ctlg_electroniz.sch_domain_finance_gold.aggregated_sales_by_year AS
SELECT
  year,
  SUM(aggregated_sales_price) AS aggregated_sales_price
FROM
  (
    SELECT
      YEAR(order_date) AS year,
      round(sum(sale_price_usd), 2) as aggregated_sales_price
    FROM
      ctlg_electroniz.sch_silver.tbl_silver_ecommerce_orders
    GROUP BY
      YEAR(order_date)
    UNION ALL
    SELECT
      YEAR(order_date) AS year,
      round(sum(sale_price_usd), 2) as aggregated_sales_price
    FROM
      ctlg_electroniz.sch_silver.tbl_silver_orders
    GROUP BY
      YEAR(order_date)
  ) as aggregated_sales
GROUP BY
  year;