/*
==================================================================================================================================
Data Definition Language Script
Script purpose:
  This script creates tables in the 'bronze' schema, dropping existsing tables if they already exist
==================================================================================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);


IF OBJECT_ID('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;

CREATE TABLE bronze.erp_CUST_AZ12(
	CID NVARCHAR(50),
	BDATE DATE,
	GEN CHAR(50)
);

IF OBJECT_ID('bronze.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101(
CID NVARCHAR(50),
CNTRY CHAR(50)
);

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2(
ID NVARCHAR(50),
CAT CHAR(50),
SUBCAT CHAR(50),
MAINTENANCE NVARCHAR(50)
);

--Creating a Stored procedure to execute the script

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '======================================================='
		PRINT 'Loading Bronze Layer'
		PRINT '======================================================='

		--Truncate & Inserting from Source files

		SET @start_time = GETDATE();
		PRINT 'Loading CRM Data'
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\mark.mulehi\Desktop\Datawarehouse\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\mark.mulehi\Desktop\Datawarehouse\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\mark.mulehi\Desktop\Datawarehouse\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT 'Loading ERP Data'
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\mark.mulehi\Desktop\Datawarehouse\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_LOC_A101;
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\mark.mulehi\Desktop\Datawarehouse\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\mark.mulehi\Desktop\Datawarehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'

		END TRY
	BEGIN CATCH
	PRINT '==============================================================================='
	PRINT 'Error Loading bronze layer'
	PRINT '==============================================================================='
	END CATCH
END
