-- Best customers
SELECT
    c.CustomerID AS customer_id,
    MAX(od.Quantity) AS max_quantity_ordered
FROM
    orders_staging o
JOIN
    order_details_staging od ON o.OrderID = od.OrderID
JOIN
    customers_staging c ON o.CustomerID = c.CustomerID
GROUP BY
    c.CustomerID
ORDER BY
    max_quantity_ordered DESC
LIMIT 10;

-- Best employeess
SELECT
    e.FirstName || ' ' || e.LastName AS employee_name,
    COUNT(o.OrderID) AS total_orders
FROM
    orders_staging o
JOIN
    employees_staging e ON o.EmployeeID = e.EmployeeID
GROUP BY
    e.FirstName, e.LastName
ORDER BY
    total_orders DESC
LIMIT 10;

-- Best shipper
SELECT
    s.CompanyName AS shipper_name,
    COUNT(o.OrderID) AS total_orders
FROM
    shippers_staging s
LEFT JOIN
    orders_staging o ON s.ShipperID = o.ShipVia
WHERE
    s.CompanyName IN ('Speedy Express', 'United Package', 'Federal Shipping', 'Alliance Shippers', 'UPS', 'DHL')
GROUP BY
    s.CompanyName
ORDER BY
    total_orders DESC;

-- Best top3 product low3 product by price
(SELECT
    ProductName,
    UnitPrice
FROM
    products_staging
ORDER BY
    UnitPrice ASC
LIMIT 3)

UNION ALL

(SELECT
    ProductName,
    UnitPrice
FROM
    products_staging
ORDER BY
    UnitPrice DESC
LIMIT 3);

-- Best top5 suplier
SELECT
    s.CompanyName AS supplier_name,
    SUM(od.Quantity) AS total_quantity_ordered
FROM
    suppliers_staging s
JOIN
    products_staging p ON s.SupplierID = p.SupplierID
JOIN
    order_details_staging od ON p.ProductID = od.ProductID
GROUP BY
    s.CompanyName
ORDER BY
    total_quantity_ordered DESC
LIMIT 5;
