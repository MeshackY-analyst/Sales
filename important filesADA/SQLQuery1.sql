/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouseAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouseAnalytics')
BEGIN
    ALTER DATABASE DataWarehouseAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouseAnalytics;
END;
GO

-- Create the 'DataWarehouseAnalytics' database
CREATE DATABASE DataWarehouseAnalytics;
GO

USE DataWarehouseAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_customers(
	customer_key int,
	customer_id int,
	customer_number nvarchar(50),
	first_name nvarchar(50),
	last_name nvarchar(50),
	country nvarchar(50),
	marital_status nvarchar(50),
	gender nvarchar(50),
	birthdate date,
	create_date date
);
GO

CREATE TABLE gold.dim_products(
	product_key int ,
	product_id int ,
	product_number nvarchar(50) ,
	product_name nvarchar(50) ,
	category_id nvarchar(50) ,
	category nvarchar(50) ,
	subcategory nvarchar(50) ,
	maintenance nvarchar(50) ,
	cost int,
	product_line nvarchar(50),
	start_date date 
);
GO

CREATE TABLE gold.fact_sales(
	order_number nvarchar(50),
	product_key int,
	customer_key int,
	order_date date,
	shipping_date date,
	due_date date,
	sales_amount int,
	quantity tinyint,
	price int 
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\Users\Meshack\Desktop\important filesADA\gold.dim_customers.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\Users\Meshack\Desktop\important filesADA\gold.dim_products.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO

TRUNCATE TABLE gold.fact_sales;
GO

BULK INSERT gold.fact_sales
FROM 'C:\Users\Meshack\Desktop\important filesADA\gold.fact_sales.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO
--Change Over time trends ================================================================================
Use DataWarehouseAnalytics;

SELECT Year(order_date) as yearly_sales,
month(order_date) as order_month,
Count(distinct Customer_key) as total_customers,
sum(quantity) as total_quantity,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by Year(order_date),month(order_date)
order by Year(order_date),month(order_date);


--datetrunc for more specifics 

SELECT datetrunc(year ,order_date) as order_date,
Count(distinct Customer_key) as total_customers,
sum(quantity) as total_quantity,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by datetrunc(year ,order_date)
order by datetrunc(year ,order_date);


--cummulative analysis====================================================================================
--we use aggrigate windows function
-- how your business is growing ?

Select DATETRUNC(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where DATETRUNC(month,order_date)  is not null
group by DATETRUNC(month,order_date) 
order by DATETRUNC(month,order_date) 
;

-- calculating rolling total 
select order_date,
total_sales,
sum(total_sales)over (order by order_date) as Rolling_sum
from
(
Select DATETRUNC(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where DATETRUNC(month,order_date)  is not null
group by DATETRUNC(month,order_date) 

) t

--moving average 

select order_date,
total_sales,
sum(total_sales)over (order by order_date) as Rolling_sum,
avg(avg_price) over (order by order_date) as moving_avg
from
(
Select DATETRUNC(year,order_date) as order_date,
sum(sales_amount) as total_sales,
avg(price) as Avg_price
from gold.fact_sales
where (order_date)  is not null
group by DATETRUNC(year,order_date) 

) t

-- performance Value ===================================================================================
-- current[measure]- target[measure]
-- above average ,below average 

select 
year(f.order_date) as order_date,
p.product_name,
sum(f.sales_amount) total_amount from 
gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null 
group by
year(f.order_date),p.product_name

/* Analyze the Yearly performance of products by comparing their sales to both the average sales 
performance of the product and the previous year's sales */

with yearly_product_sales AS (
select 
year(f.order_date) as order_date,
p.product_name,
sum(f.sales_amount) as  current_sales from 
gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null 
group by
year(f.order_date),p.product_name
)
SELECT
order_date,
product_name,
current_sales,

avg(current_sales) OVER (PARTITION BY product_name) avg_sales,
current_sales - avg(current_sales) over (PARTITION BY product_name) as diff_avgs,
case when current_sales - avg(current_sales) over (PARTITION BY product_name) >0 then 'Above Avg'
	 when current_sales - avg(current_sales) over (PARTITION BY product_name) <0 then 'below_avg'
	 else 'avg'
end avg_change,
-- year to year analysis
lag(current_sales) over (PARTITION BY product_name order by order_date) py_sales,
current_sales -lag(current_sales) over (PARTITION BY product_name order by order_date) diff_py,
case when current_sales -lag(current_sales) over (PARTITION BY product_name order by order_date)>0 then 'Increase'
	 when current_sales -lag(current_sales) over (PARTITION BY product_name order by order_date)<0 then 'Decrease'
	 else 'no change'
end py_change
from yearly_product_sales
order by product_name,order_date


-- Part to Whole Analysis 
-- how is individual category effecting other categories 
select category,
sum(sales_amount)  total_amount
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
group by category

with category_sales as (
SELECT
category,
sum(sales_amount)  total_amount
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
group by category)
SELECT category,
total_amount ,
sum(total_amount) over() rolled_total,
CONCAT(ROUND((CAST( total_amount AS FLOAT) / sum(total_amount) over())*100,2),'%')as percent_div
from category_sales
order by total_amount desc


-- Data Segmentation =========================================================================================================================
/* segments products into cost range and count hoe many products fall into each category*/
USE DataWarehouseAnalytics
with product_segments as (
SELECT 
product_key,
product_name,
cost,
CASE WHEN cost < 100 then 'Less than 100'
     WHEN cost BETWEEN 100 and 500 then '100-500'
	 WHEN cost BETWEEN 500 and 1000 then '500-1000'
	 ELSE 'more than 1000'
END COST_CHANGE
from gold.dim_products)
select COST_CHANGE,
count(product_key) as total_products
from product_segments
group by COST_CHANGE
order by total_products

/* group customers into three segments based on their spending behaviour 
-- VIP : Customers with least 12 months of history and has transaction more the $5000
-- Regular : Customers with at least of 12 months history and but spending $5000 or less
-- New : Customers with a lifespan by each group 
*/

with customer_spending as (
select c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
datediff(month,min(order_date),max(order_date)) as lifespan
from 
gold.fact_sales f 
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key
)
SELECT customer_segment,
count(customer_key) as total_customers
from(
SELECT
customer_key,
total_spending,
lifespan,
CASE WHEN lifespan >= 12 and total_spending >5000 then 'VIP'
     WHEN lifespan >= 12 and total_spending <=5000  then 'Regular'
	 Else 'New'
End Customer_segment

from customer_spending) t
group by customer_segment
order by total_customers desc

