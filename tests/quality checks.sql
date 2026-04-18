/*
==============================================================================================================
Quality Checks
==============================================================================================================
Script Purposes:
  This script perfomrs various quality checks for data consistency, accuracy, and standardization across the 
'Silver' schema. It includes check for: 
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data Standardization and consistency. 
- Invalid data ranges and orders. 
- Data consistency between related fields. 

Usage Notes: 
- Run these checks after loading Silver layer.
- Investigate and resolve any discreparencies found during the checks.
==============================================================================================================
*/

--============================================================================================================
-- Checking 'silver.crm_cust_info'
--============================================================================================================
-- Check for Nulls or Duplicates in primary key
-- Expectation: No result
select
cst_id,
count(*)
from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id IS NULL
---------------------------------------------------
--Check for unwanted Spaces
-- Expectation: No Result
select
cst_firstname
from silver.crm_cust_info
where cst_firstname !=Trim(cst_firstname)

select
cst_lastname
from silver.crm_cust_info
where cst_lastname !=Trim(cst_lastname)

select
cst_gndr
from silver.crm_cust_info
where cst_gndr !=Trim(cst_gndr)

----------------------------------------------------------------------
-- Data Standardization & Consistency

select 
distinct cst_gndr
from silver.crm_cust_info
--============================================================================================================
-- Checking 'silver.crm_sales_details'
--============================================================================================================

-----------------------------------------------------------
-- Check for invalid Date orders
select *
from silver.crm_sales_details
where sls_order_dt> sls_ship_dt or sls_order_dt>sls_due_dt

---------------------------------------------------------------------
--Check for correct calculations
select distinct
sls_sales, 
sls_quantity ,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales<=0 or sls_quantity<=0 or sls_price<=0
order by sls_sales,sls_quantity,sls_price

select * from silver.crm_sales_details
--============================================================================================================
-- Checking 'silver.crm_prd_info'
--============================================================================================================

select
prd_id,
prd_key,
substring(prd_key,1,5) as cat_id,
prd_nm,
prd_cost,
prd_start_dt,
prd_end_dt
from silver.crm_prd_info
---------------------------------------------------
-- Check for Nulls or Duplicated in primary Key
--Expectation: No Results
select 
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is NULL


--check for unwanted spaces 
-- Expectation: No Result 
select 
prd_nm
from silver.crm_prd_info
where prd_nm!=trim(prd_nm)

-- Check for Nulls or Negative Numbers
-- Expectation: No Results
select 
prd_cost
from silver.crm_prd_info
where prd_cost <0 or prd_cost is Null

select distinct 
prd_line
from silver.crm_prd_info

-- Check for invalid Date Orders

select *
from silver.crm_prd_info
where prd_end_dt<prd_start_dt

select *
from silver.crm_prd_info

--============================================================================================================
-- Checking 'silver.erp_cust_az12'
--============================================================================================================
select 
case when cid like 'NAS%' then substring(cid,4,len(cid)) 
	else cid
end as cid,

case when bdate>getdate() then NULL
	else bdate
end as bdate,

case when upper(trim(gen)) in ('F','Female') then 'Female' 
	 when upper(trim(gen)) in ('M','Male') then 'Male' 
	 else 'n/a'
end as gen

from silver.erp_cust_az12

---------------------------------------------------------------------------
-- To check if the transformation match with the other table
--where case when cid like 'NAS%' then substring(cid,4,len(cid)) 
	--else cid
-- end not in (select distinct cst_key from [silver].[crm_cust_info]) 
-----------------------------------------------------
--Filter to see the behavior of the column
--where cid like '%AW00011000%'
-------------------------------------------------------
-- Check with the other connected table
--select * from [silver].[crm_cust_info];
------------------------------------------------------------
--Check for outliers in the date + (remove at least the future birth dates )
select distinct 
bdate

from silver.erp_cust_az12
where bdate< '1924-01-01' or bdate>getdate()
----------------------------------------------------------------------------------
--check for all possible values
select distinct 
gen
from silver.erp_cust_az12

select  * from silver.erp_cust_az12
--============================================================================================================
-- Checking 'silver.erp_loc_a101'
--============================================================================================================

--select cst_key from silver.crm_cust_info;
---------------------------------------------------------------------
--Data Standardization & Consistency
select distinct 
cntry
 from silver.erp_loc_a101
order by cntry

select * from silver.erp_loc_a101 
--============================================================================================================
-- Checking 'silver.erp_px_gv12'
--============================================================================================================
--It was clean and needed no check.
