/*
=================================================================
DDL Script: Create Bronze Tables
=================================================================
Script Purposes: 
This Script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
Run this script to re-define the DDL structure of 'bronze' Tables.
=================================================================
*/
if OBJECT_ID ('bronze.crm_cust_info ','U') is not null
	drop table bronze.crm_cust_info ;
GO
create table bronze.crm_cust_info (
cst_id int,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_statuse NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE

);
GO
if OBJECT_ID ('bronze.crm_prd_info ','U') is not null
	drop table bronze.crm_prd_info ;
GO
create table bronze.crm_prd_info (
prd_id int,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost int,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME


);
GO
if OBJECT_ID ('bronze.crm_sales_details ','U') is not null
	drop table bronze.crm_sales_details ;
GO
create table bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int

);
GO
if OBJECT_ID ('bronze.erp_loc_a101 ','U') is not null
	drop table bronze.erp_loc_a101 ;
GO
create table bronze.erp_loc_a101 (
cid NVARCHAR(50),
cntry NVARCHAR(50)
);
GO

if OBJECT_ID ('bronze.erp_cust_az12 ','U') is not null
	drop table bronze.erp_cust_az12 ;
GO
create table bronze.erp_cust_az12 (
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)

);
GO

if OBJECT_ID ('bronze.erp_px_cat_g1v2 ','U') is not null
	drop table bronze.erp_px_cat_g1v2 ;
GO
create table bronze.erp_px_cat_g1v2 (
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);

GO
