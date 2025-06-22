-- This SQL script calculates the average discount for each product line.
-- It assumes a 'sales_data_cleaned' table exists, containing the raw sales data
-- after initial cleaning and the 'DISCOUNT' column has been calculated.

SELECT
    ProductLine,
    ROUND(AVG(DISCOUNT), 4) AS AverageDiscount
FROM
    sales_data_cleaned
GROUP BY
    ProductLine
ORDER BY
    AverageDiscount DESC;
