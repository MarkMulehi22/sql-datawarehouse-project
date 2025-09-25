/*
================================================================================================================================================
Stored Procedure: Load Silver layer

================================================================================================================================================
Script Purpose:
  This stored procedure performs ETL process to populate the 'silver' schema table from the 'bronze' schema.
Actions performed:
  -Truncates the silver table to update in the event of new data
  -Inserts transs=formed and cleaned data from Bronze into Silver tables
=================================================================================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT '======================================================='
		PRINT 'Loading Silver Layer'
		PRINT '======================================================='

		--Removing unwanted spaces and removing duplicates and  inserting clean data to the silver schema for crm_cust_info
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info ;
		PRINT '>> Inserting Data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_gndr,
		cst_marital_status,
		cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		--Removing unwanted spaces
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		--Data Normalization and standadization
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
			WHEN cst_gndr IS NULL THEN 'n/a'
			END AS cst_gndr,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
			WHEN cst_marital_status IS NULL THEN 'n/a'
			END AS cst_marital_status,
		cst_create_date
		FROM (
		--Handles duplicate values by using window function ROW_NUMBER grouping by cst_id
		SELECT *, 
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id is NOT NULL
			) t
			WHERE flag_last = 1
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'


		--SPLITTING prd-key to category ID and replacing - with _ and inserting into Silver schema
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'Inserting Data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'n/a'
			 END AS prd_line,
		prd_start_dt,
		DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'


		--Cleaning and checking of bronze.crm_sales_details
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'Inserting data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		--Converting column sls_order_dt to DATE format from int and ignoring wrongly input values
		CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'


		--cleaning and loading bronze.erp_CUST_AZ12
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		PRINT 'Inserting data into silver.erp_CUST_AZ12';
		INSERT INTO silver.erp_CUST_AZ12(
		cid,
		bdate,
		gen
		)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen))='F' THEN 'Female'
			WHEN UPPER(TRIM(gen))= 'M'THEN 'Male'
			WHEN gen IS NULL THEN 'n/a'
			WHEN gen = ' ' THEN 'n/a'
			ELSE gen
		END AS gen
		FROM bronze.erp_CUST_AZ12
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'


		--Cleaning and loading bronze.erp_LOC_A101
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_LOC_A101;
		PRINT 'Inserting into silver.erp_LOC_A101';
		INSERT INTO silver.erp_LOC_A101(
		cid,
		cntry
		)
		SELECT
		REPLACE( cid, '-', ''),
		CASE WHEN cntry = 'US' or cntry = 'USA'THEN 'United States'
			WHEN cntry = 'DE' THEN 'Denmark'
			WHEN cntry = ' ' THEN 'n/a'
			WHEN cntry IS NULL then 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_LOC_A101
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'


		--Cleaning and loading bronze.erp_PX_CAT_G1V2
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT 'Inserting into silver.erp_PX_CAT_G1V2';
		INSERT INTO silver.erp_PX_CAT_G1V2(
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		)
		SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM bronze.erp_PX_CAT_G1V2
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION:' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '----------------------------------------------------------------------------'
	END TRY
	BEGIN CATCH
	PRINT '==============================================================================='
	PRINT 'Error Loading silver'
	PRINT '==============================================================================='
	END CATCH
END
