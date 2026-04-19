/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view gold.dim_customers AS --8. Create the object
Select
Row_number() over (order by cst_id) As customer_key, -- 7. Generate Surrogate key
ci.cst_id As customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname As first_name,
ci.cst_lastname As last_name,
la.cntry As country,
ci.cst_marital_status As marital_status,
CASE WHEN ci.cst_gndr!= 'n/a' Then ci.cst_gndr --CRM is the Master for gender
	Else COALESCE(ca.gen,'n/a')
End As gender,
--ci.cst_create_date As create_date,
ca.bdate As birthdate,
ci.cst_create_date As create_date
--la.cntry As country
From silver.crm_cust_info ci
Left Join silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
Left Join silver.erp_loc_a101 la
ON ci.cst_key=la.cid  
GO
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
  
create view gold.dim_products AS -- Create View
Select
ROW_Number() over (order by pn.prd_start_dt,pn.prd_key) AS product_key,
pn.prd_id As product_id,
pn.prd_key As product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category, 
pc.subcat AS subcategory,
pc.maintenance,
--pn.prd_key, 4: sort the columns to logical groups
--pn.prd_nm,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
--pn.prd_end_dt AS end_date
From silver.crm_prd_info pn
Left Join silver.erp_px_cat_g1v2 pc --2.Left Join the product categories
ON pn.cat_id=pc.id
where prd_end_dt IS NULL -- 1.Filter out all historical data & keep only current data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

Create or Alter view gold.fact_sales AS
select
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_dare,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
From silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id

Go


