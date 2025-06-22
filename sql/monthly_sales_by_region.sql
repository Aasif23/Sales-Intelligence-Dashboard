-- This SQL script calculates the total sales for each month, grouped by year and country.
-- It uses SQLite's strftime function to extract year and month from the OrderDate.
-- It assumes a 'sales_data_cleaned' table exists with 'OrderDate', 'Country', and 'Sales' columns.

SELECT
    strftime('%Y', OrderDate) AS SalesYear,
    strftime('%m', OrderDate) AS SalesMonth,
    Country,
    ROUND(SUM(Sales), 2) AS TotalSales
FROM
    sales_data_cleaned
GROUP BY
    SalesYear,
    SalesMonth,
    Country
ORDER BY
    SalesYear,
    SalesMonth,
    TotalSales DESC;
