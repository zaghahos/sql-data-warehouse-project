/*
==========================================================================================
Stored Procedure: Load Bronze Layer (Source-> Bronze)
==========================================================================================
Script Purpose: 
  This stored procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  -Truncated the bronze tables before loading data.
  - Uses the 'Bulk Insert' command to load data from csv files to bronze tables. 

Parameters:
None. 
This stored procedure does not accept any parameters or return any values. 

Usage Example: 
EXEC bronze.load_bronze;
===========================================================================================
*/

Create or Alter procedure bronze.load_bronze AS
Begin
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 

	Begin TRY
		set @batch_start_time=GETDATE();
		Print '==============================================================================';
		Print 'Loading Bronze Layer';
		Print '==============================================================================';

		print '------------------------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '------------------------------------------------------------------------------';
	    
		Set @start_time=GETDATE();
		print '>> Truncating Tables:bronze.crm_cust_info ';
		Truncate Table bronze.crm_cust_info;
		print '>> Inserting Data into:bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\SQL_Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		Firstrow=2, 
		fieldterminator=',' ,
		TABLOCK 
		);
	 Set @end_time=GETDATE();
	 print '>> Load Duration:'+Cast(Datediff(second, @start_time, @end_time) As NVARCHAR)+ 'Seconds';
	 print '>>--------------';


		--select * from bronze.crm_cust_info

		--select count(*) from bronze.crm_cust_info
		----------------------------------------------------------------------------------------------------
		set @start_time=GETDATE();
		print '>> Truncating Tables:bronze.crm_prd_info ';
		Truncate Table bronze.crm_prd_info;
		print '>> Inserting Data into:bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\SQL_Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		Firstrow=2, 
		fieldterminator=',' ,
		TABLOCK 
		);
	   Set @end_time=GETDATE();
	   print '>> Load Duration:'+Cast(Datediff(second, @start_time, @end_time) As NVARCHAR)+ 'Seconds';
	   print '>>--------------';
		--select * from bronze.crm_prd_info

		--select count(*) from bronze.crm_prd_info
		----------------------------------------------------------------------------------------------------
		set @start_time=GETDATE();
		print '>> Truncating Tables:bronze.crm_sales_details ';
		Truncate Table bronze.crm_sales_details;
		print '>> Inserting Data into:bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\SQL_Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		Firstrow=2, 
		fieldterminator=',' ,
		TABLOCK 
		);

		Set @end_time=GETDATE();
		print '>> Load Duration:'+Cast(Datediff(second, @start_time, @end_time) As NVARCHAR)+ 'Seconds';
		print '>>--------------';
		--select * from bronze.crm_sales_details

		--select count(*) from bronze.crm_sales_details

		----------------------------------------------------------------------------------------------------
		print '------------------------------------------------------------------------------';
		print 'Loading ERP Tables';
		print '------------------------------------------------------------------------------';
	
	    
		set @start_time=GETDATE();
		print '>> Truncating Tables:bronze.erp_cust_az12 ';
		Truncate Table bronze.erp_cust_az12;
		print '>> Inserting Data into:bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\SQL_Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		Firstrow=2, 
		fieldterminator=',' ,
		TABLOCK 
		);
		Set @end_time=GETDATE();
		print '>> Load Duration:'+Cast(Datediff(second, @start_time, @end_time) As NVARCHAR)+ 'Seconds';
		print '>>--------------';
		--select * from bronze.erp_cust_az12

		--select count(*) from bronze.erp_cust_az12

		-----------------------------------------------------------------------------------------------------
		set @start_time=GETDATE();
		print '>> Truncating Tables:bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101;;
		print '>> Inserting Data into:bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\SQL_Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		Firstrow=2, 
		fieldterminator=',' ,
		TABLOCK 
		);

		Set @end_time=GETDATE();
		print '>> Load Duration:'+Cast(Datediff(second, @start_time, @end_time) As NVARCHAR)+ 'Seconds';
		print '>>--------------';
		--select * from bronze.erp_loc_a101

		--select count(*) from bronze.erp_loc_a101

		-------------------------------------------------------------------------------------------------------
		set @start_time=GETDATE();
		print '>> Truncating Tables:bronze.erp_px_cat_g1v2 ';
		Truncate Table bronze.erp_px_cat_g1v2;;
		print '>> Inserting Data into:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\SQL_Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		Firstrow=2, 
		fieldterminator=',' ,
		TABLOCK 
		);
		Set @end_time=GETDATE();
		print '>> Load Duration:'+Cast(Datediff(second, @start_time, @end_time) As NVARCHAR)+ 'Seconds';
		print '>>--------------';
		--select * from bronze.erp_px_cat_g1v2

		--select count(*) from bronze.erp_px_cat_g1v2


	set @batch_end_time=GETDATE();
	print '==============================================================================================';
	print 'Loading Bronze Layer is completed';
	print '  -Total Load Duration:'+Cast(Datediff(second, @start_time, @end_time) As NVARCHAR)+ 'Seconds';
	print '==============================================================================================';
	END TRY
	Begin Catch
		Print '==================================================================';
		Print 'Error Occured During Loading Bronze layer';
		Print 'Error Message'+ ERROR_Message();
		Print 'Error Message'+Cast( ERROR_number() AS NVARCHAR);
		Print 'Error Message'+Cast( ERROR_state() AS NVARCHAR);



		Print '=================================================================='

	End Catch
End
