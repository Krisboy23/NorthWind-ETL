# **ETL proces pre dátový súbor NorthWind**

## **1. Úvod a popis zdrojových dát**
Tento projekt sa zameriava na analýzu dát z datasetu **NorthWind**, ktorý obsahuje informácie o produktoch, zákazníkoch a ich transakciách. Cieľom analýzy je identifikovať trendy v správaní používateľov pri nákupoch a preferenciách produktov, ako aj analyzovať najpopulárnejšie produkty a zákaznícke správanie na základe hodnotení a demografických údajov.

Zdrojové dáta pochádzajú z datasetu dostupného na [Kaggle](https://www.kaggle.com/datasets/cleveranjosqlik/csv-northwind-database). Dataset obsahuje nasledujúce tabuľky:

- **`products`**: Obsahuje informácie o produktoch, ako sú názov, cena a kategória.
- **`orders`**: Obsahuje informácie o objednávkach, ako sú dátumy objednávok a stav objednávok.
- **`customers`**: Obsahuje údaje o zákazníkoch, ako sú meno, adresa a kontaktné informácie.
- **`categories`**: Obsahuje kategórie produktov.
- **`suppliers`**: Obsahuje informácie o dodávateľoch produktov.

Cieľom ETL procesu je pripraviť tieto dáta na analýzu a vizualizáciu v multidimenzionálnom modeli.

### **1.1 Dátová architektúra**

#### **ERD diagram**
Súčasný model zdrojových dát je znázornený v nasledujúcom **ERD (Entity Relationship Diagram)**. Tento diagram ukazuje vzťahy medzi rôznymi tabuľkami v pôvodnej štruktúre datasetu, čím sa vizualizujú spojenia medzi produktmi, zákazníkmi, objednávkami a ďalšími tabuľkami.

<p align="center">
  <img src="https://github.com/Krisboy23/NorthWind-ETL/blob/master/erd_Schema.png" alt="ERD Schéma">
  <br>
  <em>Obrázok 1: Diagram entity a vzťahov databázy NorthWind</em>
</p>

## **2. Návrh dimenzionálneho modelu**

Pre tento projekt bol navrhnutý **hviezdicový model (star schema)**, ktorý uľahčuje multidimenzionálnu analýzu. Model sa skladá z centrálnej faktovej tabuľky, ktorá je prepojená s viacerými dimenziami.

### **Hlavné metriky a kľúče vo faktovej tabuľke**
Faktová tabuľka **`fact_orders`** obsahuje kľúčové metriky ako celkový objem predaja, množstvo predaných produktov, a kľúče, ktoré odkazujú na dimenzionálne tabuľky ako zákazníka, produkt, dátum a čas transakcie.

### **Dimenzionálne tabuľky**
1. **`dim_products`**: Táto dimenzia obsahuje podrobnosti o produktoch ako názov, cena a kategória. S faktovou tabuľkou je spojená cez `product_key`.
2. **`dim_customers`**: Obsahuje údaje o zákazníkoch ako meno, adresa a veková kategória. S faktovou tabuľkou je spojená cez `customer_key`.
3. **`dim_date`**: Táto dimenzia obsahuje informácie o dátumoch transakcií (deň, mesiac, rok). S faktovou tabuľkou je spojená cez `date_key`.
4. **`dim_time`**: Obsahuje podrobné časové údaje (hodina, AM/PM). S faktovou tabuľkou je spojená cez `time_key`.

**Typy dimenzií:**
- **`dim_products`** a **`dim_customers`** sú typu **SCD Type 1**, čo znamená, že ak dôjde k zmene informácií, tieto sa aktualizujú.
- **`dim_date`** je typu **SCD Type 0**, pretože dátumy sú nemenné.
- **`dim_time`** je typu **SCD Type 0**.

  <p align="center">
  <img src="https://github.com/Krisboy23/NorthWind-ETL/blob/master/star_Schema.png" alt="Hviezdicová schéma">
  <br>
  <em>Obrázok 2: Hviezdicová schéma pre NorthWind</em>
</p>

## **3. ETL proces v nástroji Snowflake**

ETL proces pozostával z troch hlavných fáz: extrakcia (Extract), transformácia (Transform) a načítanie (Load). Tento proces bol implementovaný v Snowflake, aby sa pripravili zdrojové údaje zo staging vrstvy do multidimenzionálneho modelu vhodného na analýzu a vizualizáciu.

### **3.1 Extract (Extrahovanie dát)**

V tejto fáze sme použili príkazy na extrahovanie dát zo súborov (vo formáte CSV) a ich nahranie do staging tabuliek v Snowflake. Tento krok je kľúčový pre získanie a prípravu dát na ďalšie spracovanie.
```sql
CREATE DATABASE IF NOT EXISTS CAMEL_NortWind_DB;
USE DATABASE CAMEL_NortWind_DB;

CREATE SCHEMA IF NOT EXISTS CAMEL_NortWind_DB.STAGING;
USE SCHEMA CAMEL_NortWind_DB.STAGING;

CREATE OR REPLACE STAGE CAMEL_NortWind_STAGE;

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
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

```

### **3.2 Transform (Transformácia dát)**

Transformácia zahŕňa čistenie a spracovanie dát do požadovaného formátu. V tejto fáze sme vytvorili dimenzionálne tabuľky a pripravili dáta na analýzu.
```sql
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
```

### **3.3 Load (Načítanie dát)**

Po úspešnom vytvorení dimenzionálnych tabuliek sme načítali dáta do finálnych tabuliek, ktoré sú pripravené na analýzu. Tento krok uzatvára ETL proces.
```sql
INSERT INTO final_customers
SELECT * FROM dim_customers;

INSERT INTO final_products
SELECT * FROM dim_products;
```

## **4. Vizualizácia dát**

Pre tento projekt navrhujeme nasledujúcich 5 vizualizácií, ktoré odpovedajú na dôležité otázky týkajúce sa analýzy predaja a správania používateľov:
<p align="center">
  <img src="https://github.com/Krisboy23/NorthWind-ETL/blob/master/NorthWind_dashboard.png" alt="Dashboard">
  <br>
  <em>Obrázok 3 Dashboard pre NorthWind </em>
</p>

### **Graf 1: Najlepší zákazníci**
Tento graf zobrazuje 10 zákazníkov s najvyšším počtom objednávok. Pomáha odpovedať na otázku: **Ktorí zákazníci majú najväčší objem objednávok?**

```sql
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
```

### **Graf 2: Najlepší zamestnanci**
Tento graf zobrazuje 10 zamestnancov, ktorí spravovali najväčší počet objednávok. Pomáha odpovedať na otázku: **Ktorí zamestnanci spravujú najviac objednávok?**

```sql
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
```

### **Graf 3: Najlepší dopravcovia**
Tento graf ukazuje, ktorí dopravcovia spracovali najväčší počet objednávok. Pomáha odpovedať na otázku: **Ktorí dopravcovia majú najväčší objem objednávok?**

```sql
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
```

### **Graf 4: Top 3 najlacnejšie a top 3 najdrahšie produkty**
Tento graf zobrazuje 3 najlacnejšie a 3 najdrahšie produkty v databáze. Odpovedá na otázku: **Aké sú najlacnejšie a najdrahšie produkty v ponuke?**

```sql
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
```

### **Graf 5: Top 5 najlepších dodávateľov**
Tento graf zobrazuje 5 dodávateľov s najvyšším množstvom predaného tovaru. Odpovedá na otázku: **Ktorí dodávatelia generujú najvyšší objem predaja?**

```sql
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
```
---

**Autor:** Kristóf Mag

