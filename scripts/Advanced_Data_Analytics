/* 
==========================================================================================
Advanced Data Analytics 
==========================================================================================
Purpose: 
Demonstrate the most important aspects of data analytics in SQL including :
	1. Change Over Time Analysis
	2. Cumulative Analysis
	3. Performance Analysis
	4. Part-to-Whole Analysis
	5. Data Segmentation
	6. Data Reporting 
		** Customer Report
		** Product Report

===========================================================================================

*/



--1. Change over time Analysis:  Analyze the Sales performance over time 
-- Used three approached for getting the trends based on year, month, or year & month
 -- Aggregate Days to Years
select 
year(order_date) as order_year,
sum(sales_amount) as total_Sales,
count (Distinct customer_key) AS total_customers,
sum(quantity) As total_quantity
From gold.fact_sales
where order_date IS NOT NULL
group by year(order_date)
order by year(order_date)

------------------------------------------------------------------------
 -- Aggregate Days to Months

select 
month(order_date) as order_month,
sum(sales_amount) as total_Sales,
count (Distinct customer_key) AS total_customers,
sum(quantity) As total_quantity
From gold.fact_sales
where order_date IS NOT NULL
group by month(order_date)
order by month(order_date)

----------------------------------------------------------------------------
 -- Aggregate Days to Years And Months

select 
year(order_date) as order_year,
month(order_date) as order_month,
sum(sales_amount) as total_Sales,
count (Distinct customer_key) AS total_customers,
sum(quantity) As total_quantity
From gold.fact_sales
where order_date IS NOT NULL
group by year(order_date), month(order_date)
order by year(order_date), month(order_date)
----------------------------------------------------------------------------
 -- Aggregate Days to Years And Months USING DATETRUNC() 

select 
DATETRUNC(month,order_date) as order_date,
sum(sales_amount) as total_Sales,
count (Distinct customer_key) AS total_customers,
sum(quantity) As total_quantity
From gold.fact_sales
where order_date IS NOT NULL
group by DATETRUNC(month,order_date)
order by DATETRUNC(month,order_date)

----------------------------------------------------------------------------
 -- Aggregate Days to Years And Months USING FORMAT() 

select 
FORMAT(order_date,'yyyy-MMM') as order_date,
sum(sales_amount) as total_Sales,
count (Distinct customer_key) AS total_customers,
sum(quantity) As total_quantity
From gold.fact_sales
where order_date IS NOT NULL
group by FORMAT(order_date,'yyyy-MMM')
order by FORMAT(order_date,'yyyy-MMM')
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
--2. Cumulative Analysis: 
-- Calculate the total sales per month & the running total of sales over time : ruuning total (window Function)
Select 
order_date,
total_sales,
--Window Function
--sum(total_sales) over (partition by order_date order by order_date) As running_total_sales,
sum(total_sales) over (order by order_date) As running_total_sales,
Avg(avg_price) over (order by order_date) AS moving_average_price
From 
(
Select
--DATETRUNC(month, order_date) AS order_date,
DATETRUNC(year, order_date) AS order_date,
sum(sales_amount) As total_sales,
AVG(price) As avg_price
From gold.fact_sales 
where order_date IS NOT NULL
--Group by DATETRUNC(month, order_date)
Group by DATETRUNC(year, order_date)
) t
---------------------------------------------------------------------------------------
-- 3. Performance Analysis:
-- Analyze the yearly performancde of products (need order date) by comparing each product's sales to both its average
-- sales performance and the previous year's sales. 

With yearly_product_sales AS (

Select 
Year(f.order_date) AS order_year,
p.product_name,
sum(f.sales_amount) As current_sales
From gold.fact_sales f
Left Join gold.dim_products p
ON f.product_key=p.product_key
Where order_date IS NOT NULL
group by
Year(f.order_date), p.product_name
) 
Select
order_year,
product_name,
current_sales,
Avg(current_sales) over (partition by product_name) avg_sales,
current_sales- Avg(current_sales) over (partition by product_name) As diff_avg,
case when current_sales- Avg(current_sales) over (partition by product_name)>0 then 'Above Avg'
	 when current_sales- Avg(current_sales) over (partition by product_name)<0 then 'Below Avg' 
	 Else 'Avg'
End avg_change,
-- Year-over-Year Analysis
LAG(current_sales) over(partition by product_name order by order_year) AS py_sales,
current_sales - LAG(current_sales) over(partition by product_name order by order_year) AS diff_py,
case when current_sales- LAG(current_sales) over (partition by product_name order by order_year)>0 then 'Increase'
	 when current_sales- LAG(current_sales) over (partition by product_name order by order_year)<0 then 'Decrease' 
	 Else 'No Change'
End py_change

From yearly_product_sales
order by product_name,order_year

----------------------------------------------------------------------------------------------------------------
-- 4. Part-to-Whole Analysis
-- Which categories contribute the most to overall sales
With category_sales AS (

Select  
category,
sum(sales_amount) As total_sales
From gold.fact_sales f
Left join gold.dim_products p
ON f.product_key=p.product_key
group by category
)
Select 
category, 
total_sales,
SUM(total_sales) over () AS overall_sales,
CONCAT(Round ((CAST(total_sales AS FLOAT)/SUM(total_sales) over () ) *100,2),'%' ) AS percentage_of_total
From category_sales
order by total_sales DESC 

----------------------------------------------------------------------------------------------------------------------------
-- 5. Data Segmentation
-- Segment products into cost ranges and count how many products fall into each segment 
WITH product_segments AS (
Select
product_key,
product_name,
cost,
CASE WHEN cost<100 Then 'Below 100'
	 When cost BETWEEN 100 AND 500 THEN '100-500'
	 When cost BETWEEN 500 AND 1000 THEN '500-1000'
	 ELSE 'Above 1000'
End cost_range
From gold.dim_products
)
Select 
cost_range,
count(product_key) AS total_products
From product_segments
group by cost_range
order by total_products DESC

----------------------------------------------
-- Group customers into three segments based on their spending behavior. 
-- * VIP: at least 12 months of history and spending more than $5000.
-- * Regular: at least 12 months of history but spending $5000 or less.
-- * New: lifespan less than 12 months.
-- And find the total number of customers by each group.
WITH customer_spending AS (

Select 
c.customer_key, 
SUM(f.sales_amount) AS total_spending,
-- To calculate the lifespan--
MIN(order_date) AS first_order,
MAX(order_date) AS last_order,
DATEDIFF(month,MIN(order_date),MAX(order_date)) AS lifespan
From gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key=c.customer_key
Group by c.customer_key
)

Select 
customer_segment,
COUNT(customer_key) AS total_customers
From (
	Select 
	customer_key,
	CASE WHEN lifespan >12 AND total_spending > 5000 THEN 'VIP'
		 When lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
		 ELSE 'NEW' 
	End AS customer_segment
	From customer_spending
	) t
Group by customer_segment
Order by total_customers DESC

-----------------------------------------------------------------------------------------------------------
-- 6. Reporting
/*
================================================================================================
Customer Report
================================================================================================
Purpose: 
- This report consolidates key customer metrics and behaviors

Highlights: 
1. Gathetr essential fields such as names, ages, and transaction details. 
2. Segments customers into categories (VIP, Regular, New) and age groups. 
3. Aggregate customer-level metrics: 
	- total orders
	- total sales
	- total quantity purchased
	- total products
	- lifespan (in months)
4. Calculates valuable KPIs: 
	- recency (months since last order)
	- average order value
	- average monthly spend
================================================================================================
*/
CREATE VIEW gold.report_customers AS

With base_query AS (
/* ---------------------------------------------------------------------------------------------
1) Base Query: Retrieve core columns from tables
------------------------------------------------------------------------------------------------*/

Select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) AS customer_name,
DATEDIFF(year,c.birthdate,GETDATE()) As age
From gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key=c.customer_key
Where order_date IS NOT NULL
) 

, customer_aggregation AS (
/*-------------------------------------------------------------------------------
2) Customer Aggregations: Summarize key metrics at the customer level
---------------------------------------------------------------------------------*/
Select 
customer_key,
customer_number,
customer_name,
age,
Count(Distinct order_number) AS total_orders,
Sum(sales_amount) AS total_sales,
Sum(quantity) AS total_quantity,
Count(Distinct product_key) AS total_products,
Max(order_date) AS last_order_date,
DATEDIFF(month,MIn(order_date),MAX(order_date)) AS lifespan
From base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age
)

Select 
    customer_key,
	customer_number,
	customer_name,
	age,
	CASE 
		 When age < 20 Then 'Under 20'
		 When age Between 20 and 29 Then '20-29'
		 When age Between 30 and 39 Then '30-39'
         When age Between 40 and 49 Then '40-49'
		 ELSE '50 and above'
End As age_group,

	CASE 
		 WHEN lifespan >12 AND total_sales > 5000 THEN 'VIP'
		 When lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		 ELSE 'NEW' 
	End AS customer_segment,
	DATEDIFF(month, last_order_date,GETDATE()) AS receny,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
	-- Compute average order value (AVO)
	CASE WHEN total_orders=0 Then 0
		 ELSE total_sales / total_orders 
	End AS avg_order_value,
	-- Compute average monthly spend
	CASE WHEN lifespan=0 Then total_sales
		 ELSE total_sales / lifespan 
	End AS avg_monthly_spend
From customer_aggregation 

--Select * From gold.report_customers

-----------------------------------------------------------
-- Sample of using the above created view --
Select 
customer_segment,
Count(customer_number) AS total_customers,
SUM(total_sales) AS total_sales
From gold.report_customers
Group by customer_segment
------------------------------------------------------------------------------------------------------

-- 7. Reporting_2
/*
================================================================================================
Product Report
================================================================================================
Purpose: 
- This report consolidates key product metrics and behaviors

Highlights: 
1. Gathetr essential fields such as product names, category, subcategory, and cost. 
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers. 
3. Aggregate product-level metrics: 
	- total orders
	- total sales
	- total quantity sold
	- total customers (unique)
	- lifespan (in months)
4. Calculates valuable KPIs: 
	- recency (months since last sale)
	- average order revenue (AOR)
	- average monthly revenue
================================================================================================
*/
CREATE or Alter VIEW gold.report_products AS

with base_query_products AS 
(
-- base_query
Select 
f.order_number,
f.order_date,
f.customer_key,
f.sales_amount,
f.quantity,
p.product_key,
p.product_name,
p.product_number,
p.category,
p.subcategory,
p.cost

From gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key=p.product_key
Where order_date IS NOT NULL

)
-- Product Aggregation --
, product_aggregation AS 
(

Select 
    product_key,
	product_name,
	product_number,
	category,
	subcategory,
	cost,
    Count(Distinct order_number) AS total_orders,
	Sum(sales_amount) AS total_sales,
	Sum(quantity) AS total_quantity,
	Count( Distinct customer_key) AS total_customers,
	DATEDIFF(month,MIN(order_date),MAX(order_date)) AS lifespan ,
	MAX(order_date) AS last_sale_date,
	Round(AVG(CAST(sales_amount AS Float) /Nullif(quantity,0)),1) AS avg_selling_price

From base_query_products
group by 
	product_key,
	product_name,
	product_number,
	category,
	subcategory,
	cost
) 
-- Product Segmentation & KPIs
select 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(month,last_sale_date,GETDATE()) AS recency_in_months,
	CASE 
		WHEN total_sales > 50000 Then 'High-Performer'
		When total_sales >= 10000 Then 'Mid-Range'
		Else 'Low-Performer'
   End As product_segments,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	DATEDIFF(month,last_sale_date,GETDATE()) AS recency,
	-- Compute average order revenue (AOR)
	CASE WHEN total_orders = 0 Then 0
		 ELSE total_sales / total_orders 
	End AS avg_order_revenue,
	-- Compute average monthly revenue
	CASE WHEN lifespan=0 Then total_sales
		 ELSE total_sales / lifespan 
	End AS avg_monthly_revenue

From product_aggregation


--Select * From gold.report_products


