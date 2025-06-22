-- This SQL script identifies the top 10 customers based on their total profit.
-- It assumes a 'sales_data_cleaned' table exists with 'CustomerName' and 'Profit' columns.

SELECT
    CustomerName,
    ROUND(SUM(Profit), 2) AS TotalProfit
FROM
    sales_data_cleaned
GROUP BY
    CustomerName
ORDER BY
    TotalProfit DESC
LIMIT 10;
