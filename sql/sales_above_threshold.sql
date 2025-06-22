-- This SQL script retrieves individual orders with sales amounts exceeding a specified threshold.
-- The threshold is conceptually set to $5,000, aligning with the data pipeline's logic.
-- It assumes a 'sales_data_cleaned' table exists with 'OrderNumber', 'OrderDate',
-- 'CustomerName', 'ProductLine', and 'Sales' columns.

SELECT
    OrderNumber,
    OrderDate,
    CustomerName,
    ProductLine,
    Sales
FROM
    sales_data_cleaned
WHERE
    Sales > 5000 -- Threshold for high-value orders
ORDER BY
    Sales DESC;
