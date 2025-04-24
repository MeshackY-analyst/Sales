/* Product report for Analysis
========================================================================================================
Purpose:-
- this report consolidates key product metrics and behaviours 
Highlights-
  1.Gather essential field such as product name ,category ,sub-category and cost .
  2.segment product by revenue to identify high-performance ,mid range or low Performance.
  3.aggregate product level matrics :-
	-total order
	-total sales 
	-total quantity sold 
	-total customers(unique)
	-lifespan in months 
  4.Calculate valuable KPIs
    -Recency(months since last sale )
	-Average Order Revenue 
	- Average Monthly Revenue 
==============================================================================================================

*/
-- 1)Base Query :- Retriving core columns from tables
CREATE VIEW gold.report_products AS 
With Product_Report AS (SELECT 
f.order_number,
f.product_key,
f.customer_key,
f.order_date,
f.sales_amount,
f.quantity,
f.price,
p.product_id,
p.product_number,
p.product_name,
p.category,
p.subcategory,
p.cost
from
gold.fact_sales f 
Left Join gold.dim_products p
on f.product_key = p.product_key
WHERE order_date IS NOT NULL 
), product_aggregation AS (
SELECT
product_key,
product_name,
 /*3.aggregate product level matrics :-
	-total order
	-total sales 
	-total quantity sold 
	-total customers(unique)
	-lifespan in months */
COUNT(DISTINCT order_number) as total_orders,
SUM(sales_amount) as total_sales,
SUM(quantity) as total_quantity ,
COUNT(DISTINCT customer_key) as total_customers,
MAX(order_date) as last_order_date,
DATEDIFF(Month,MIN(order_date),MAX(order_date)) As lifespan

from Product_report
group by product_key,
product_name
)
SELECT
product_key,
product_name,
total_orders,
total_quantity,
total_customers,
last_order_date,
DATEDIFF(MONTH,last_order_date,GETDATE()) AS Recency,
lifespan,
total_sales,
case when total_orders < 1414 then 'Low_performance'
     when total_orders between 1414 and 2828 then 'Mid_performance'
	 Else 'High_performance'
END AS Performance_by_order
from Product_aggregation

