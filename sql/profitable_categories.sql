-- This SQL script identifies and ranks product categories by their total profit.
-- It filters for categories that have generated a positive profit.
-- It assumes a 'sales_data_cleaned' table exists with 'ProductLine' and 'Profit' columns.

SELECT
    ProductLine,
    ROUND(SUM(Profit), 2) AS TotalProfit
FROM
    sales_data_cleaned
GROUP BY
    ProductLine
HAVING
    SUM(Profit) > 0
ORDER BY
    TotalProfit DESC;
