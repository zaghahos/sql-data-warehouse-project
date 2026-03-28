/*
===================================================================
Create Database and Schemas
===================================================================
Script Purposes: 
This script creates a new database named 'DataWarehouse' after checking if it already exists. 
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
*/

-- Create Database 'DataWarehouse'

use master;


--Drop and recreate the DataWaehouseMarch' database 
if exists (select 1 from sys.databases where name='DataWarehouse')
begin 
	Alter Database DataWarehouse set single_user with rollback immediate;
	drop Database DataWarehouse;
end;
GO

CREATE DATABASE DataWarehouse; 

USE DataWarehouse;

--Create Schemas 

CREATE SCHEMA bronze;
Go
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


  
