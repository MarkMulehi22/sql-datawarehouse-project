/*
======================================================================================
Create Database and Schemas
======================================================================================
Script Purpose:
This script creates a new databse named 'Datawarehouse' after checking if it already exists. If the database exists, it is dropped and recreated.
Additionally the script sets up three schemas within the database: 'bronze', 'silver', and  'gold'.

Warning: if you have any database named Datwarehouse it will be deleted, please ensure you have a back up before running this script.
*/

USE master;
GO
--DROP Existing Database in case it is tere
IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
	END;

--Create database
CREATE DATABASE DataWarehouse;

--Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
