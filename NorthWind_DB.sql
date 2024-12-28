CREATE DATABASE IF NOT EXISTS CAMEL_NortWind_DB;
USE DATABASE CAMEL_NortWind_DB;

CREATE SCHEMA IF NOT EXISTS CAMEL_NortWind_DB.STAGING;
USE SCHEMA CAMEL_NortWind_DB.STAGING;

CREATE OR REPLACE STAGE CAMEL_NortWind_STAGE

-- 1) categories.csv
CREATE OR REPLACE TABLE categories_staging (
    CategoryID   NUMBER,
    CategoryName VARCHAR,
    Description  VARCHAR,
    Picture      VARCHAR
);

-- 2) customers.csv
CREATE OR REPLACE TABLE customers_staging (
    CustomerID   VARCHAR,
    CompanyName  VARCHAR,
    ContactName  VARCHAR,
    ContactTitle VARCHAR,
    Address      VARCHAR,
    City         VARCHAR,
    Region       VARCHAR,
    PostalCode   VARCHAR,
    Country      VARCHAR,
    Phone        VARCHAR,
    Fax          VARCHAR
);

ALTER TABLE customers_staging
DROP COLUMN Address;

-- 3) employees.csv
CREATE OR REPLACE TABLE employees_staging (
    EmployeeID       NUMBER,
    LastName         VARCHAR,
    FirstName        VARCHAR,
    Title            VARCHAR,
    TitleOfCourtesy  VARCHAR,
    BirthDate        DATE,  
    HireDate         DATE,  
    Address          VARCHAR,
    City             VARCHAR,
    Region           VARCHAR,
    PostalCode       VARCHAR,
    Country          VARCHAR,
    HomePhone        VARCHAR,
    Extension        VARCHAR,
    Photo            VARCHAR,
    Notes            VARCHAR,
    ReportsTo        NUMBER,
    PhotoPath        VARCHAR
);

-- 4) order_details.csv
CREATE OR REPLACE TABLE order_details_staging (
    OrderID   NUMBER,
    ProductID NUMBER,
    UnitPrice NUMBER,
    Quantity  NUMBER,
    Discount  NUMBER
);

-- 5) orders.csv
CREATE OR REPLACE TABLE orders_staging (
    OrderID        NUMBER,
    CustomerID     VARCHAR,
    EmployeeID     NUMBER,
    OrderDate      DATE,
    RequiredDate   DATE,
    ShippedDate    DATE,
    ShipVia        NUMBER,
    Freight        NUMBER,
    ShipName       VARCHAR,
    ShipCity       VARCHAR,
    ShipRegion     VARCHAR,
    ShipPostalCode VARCHAR,
    ShipCountry    VARCHAR
);


-- 6) products.csv
CREATE OR REPLACE TABLE products_staging (
    ProductID      NUMBER,
    ProductName    VARCHAR,
    SupplierID     NUMBER,
    CategoryID     NUMBER,
    QuantityPerUnit VARCHAR,
    UnitPrice      NUMBER,
    UnitsInStock   NUMBER,
    UnitsOnOrder   NUMBER,
    ReorderLevel   NUMBER,
    Discontinued   NUMBER
);

-- 7) shippers.csv
CREATE OR REPLACE TABLE shippers_staging (
    ShipperID   NUMBER,
    CompanyName VARCHAR,
    Phone       VARCHAR
);

-- 8) suppliers.csv
CREATE OR REPLACE TABLE suppliers_staging (
    SupplierID   NUMBER,
    CompanyName  VARCHAR,
    ContactName  VARCHAR,
    ContactTitle VARCHAR,
    Address      VARCHAR,
    City         VARCHAR,
    Region       VARCHAR,
    PostalCode   VARCHAR,
    Country      VARCHAR,
    Phone        VARCHAR,
    Fax          VARCHAR,
    HomePage     VARCHAR
);

-- 9) employee_territories.csv
CREATE OR REPLACE TABLE employee_territories_staging (
    EmployeeID   NUMBER,
    TerritoryID  VARCHAR
);

--10) regions.csv
CREATE OR REPLACE TABLE regions_staging (
    RegionID     NUMBER,
    RegionDescription VARCHAR
);

COPY INTO categories_staging
FROM @CAMEL_NortWind_STAGE/categories.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
);

COPY INTO customers_staging
FROM @CAMEL_NortWind_STAGE/customers.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO employees_staging
FROM @CAMEL_NortWind_STAGE/employees.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO order_details_staging
FROM @CAMEL_NortWind_STAGE/orders_details.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)


COPY INTO orders_staging
FROM @CAMEL_NortWind_STAGE/orders.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'CONTINUE';

COPY INTO products_staging
FROM @CAMEL_NortWind_STAGE/products.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO shippers_staging
FROM @CAMEL_NortWind_STAGE/shippers.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO suppliers_staging
FROM @CAMEL_NortWind_STAGE/suppliers.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO employee_territories_staging
FROM @CAMEL_NortWind_STAGE/employee_territories.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

COPY INTO regions_staging
FROM @CAMEL_NortWind_STAGE/regions.csv
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

SELECT COUNT(*) FROM categories_staging;
SELECT * FROM customers_staging LIMIT 10;

-- dim_customers
CREATE OR REPLACE TABLE dim_customers AS
SELECT DISTINCT
  CustomerID      AS customer_key,
  CompanyName     AS company_name,
  ContactName     AS contact_name,
  ContactTitle    AS contact_title,
  City            AS city,
  Region          AS region,
  PostalCode      AS postal_code,
  Country         AS country,
  Phone           AS phone,
  Fax             AS fax
FROM customers_staging;

-- dim_products 
CREATE OR REPLACE TABLE dim_products AS
SELECT DISTINCT
  p.ProductID      AS product_key,
  p.ProductName    AS product_name,
  p.UnitPrice      AS unit_price,
  p.QuantityPerUnit,
  c.CategoryName   AS category_name,
  s.CompanyName    AS supplier_name,
  s.Country        AS supplier_country,
  p.Discontinued   AS discontinued
FROM products_staging p
LEFT JOIN categories_staging c ON p.CategoryID = c.CategoryID
LEFT JOIN suppliers_staging s ON p.SupplierID = s.SupplierID;

-- dim_employees
CREATE OR REPLACE TABLE dim_employees AS
SELECT DISTINCT
  EmployeeID      AS employee_key,
  LastName        AS last_name,
  FirstName       AS first_name,
  Title           AS title,
  City            AS city,
  Country         AS country
FROM employees_staging;

-- dim_shippers
CREATE OR REPLACE TABLE dim_shippers AS
SELECT DISTINCT
  ShipperID   AS shipper_key,
  CompanyName AS company_name,
  Phone       AS phone
FROM shippers_staging;

-- fact_order_details
CREATE OR REPLACE TABLE fact_order_details AS
SELECT 
  od.OrderID,
  od.ProductID,
  od.UnitPrice,
  od.Quantity,
  od.Discount,
  o.OrderDate,
  o.RequiredDate,
  o.ShippedDate,
  o.ShipVia,
  o.Freight,
  o.CustomerID,
  o.EmployeeID
FROM order_details_staging od
JOIN orders_staging o 
    ON od.OrderID = o.OrderID;