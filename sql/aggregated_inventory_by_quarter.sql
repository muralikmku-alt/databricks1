CREATE TABLE ctlg_electroniz.sch_domain_supply_gold.aggregated_inventory_by_quarter AS
SELECT
  product_category,
  product_name,
  year,
  quarter,
  units_sold,
  inventory
FROM
  (
    SELECT
      product_category,
      product_name,
      year,
      quarter,
      SUM(units_sold) AS units_sold
    FROM
      (
        SELECT
          product_category,
          tbl_silver_products.product_name,
          YEAR(order_date) AS year,
          QUARTER(order_date) AS quarter,
          count(*) AS units_sold
        FROM
          ctlg_electroniz.sch_silver.tbl_silver_orders
            JOIN ctlg_electroniz.sch_silver.tbl_silver_products
              ON tbl_silver_products.product_id = tbl_silver_orders.product_id
        GROUP BY
          product_category,
          tbl_silver_products.product_name,
          YEAR(order_date),
          QUARTER(order_date)
        UNION ALL
        SELECT
          product_category,
          tbl_silver_products.product_name,
          YEAR(order_date) AS year,
          QUARTER(order_date) AS quarter,
          count(*) AS units_sold
        FROM
          ctlg_electroniz.sch_silver.tbl_silver_ecommerce_orders
            JOIN ctlg_electroniz.sch_silver.tbl_silver_products
              ON tbl_silver_products.product_name = tbl_silver_ecommerce_orders.product_name
        GROUP BY
          product_category,
          tbl_silver_products.product_name,
          YEAR(order_date),
          QUARTER(order_date)
      ) as aggregated_inventory_by_quarter1
    GROUP BY
      product_category,
      product_name,
      year,
      quarter
  ) as aggregated_inventory_by_quarter1
    JOIN ctlg_electroniz.sch_silver.tbl_silver_inventory
      ON tbl_silver_inventory.product = aggregated_inventory_by_quarter1.product_name;