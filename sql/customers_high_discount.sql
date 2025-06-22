-- This SQL script identifies customers who have received a high average discount.
-- It filters for customers whose average discount across their orders is greater than 10%.
-- It assumes a 'sales_data_cleaned' table exists with the 'DISCOUNT' and 'Sales' columns.

SELECT
    CustomerName,
    ROUND(AVG(DISCOUNT), 4) AS AverageDiscountUsed,
    ROUND(SUM(Sales), 2) AS TotalSales
FROM
    sales_data_cleaned
GROUP BY
    CustomerName
HAVING
    AVG(DISCOUNT) > 0.10
ORDER BY
    AverageDiscountUsed DESC;
