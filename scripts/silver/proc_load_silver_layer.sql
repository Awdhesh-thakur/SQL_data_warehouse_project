/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME , @batch_end_time DATETIME

BEGIN TRY

SET @batch_start_time = GETDATE();

PRINT '=================================';
PRINT 'Loading Silver Layer';
PRINT '=================================';

PRINT '---------------------------------';
PRINT 'Loading CRM Tables Data';
PRINT '---------------------------------';

SET @start_time = GETDATE();

PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Inserting Data Into: silver.crm_cust_info';

WITH flag_last_cte AS (
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE
            WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
            WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
            WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
)


INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)


SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM flag_last_cte
WHERE flag_last = 1;

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds'
PRINT '---------------------------------'

------------------------------

SET @start_time = GETDATE();

PRINT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Inserting Data Into: silver.crm_prd_info';

INSERT INTO silver.crm_prd_info(
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
[prd_id],
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
[prd_nm],
ISNULL([prd_cost], 0) AS prd_cost,
CASE
	WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	ELSE 'n/a'
	END
AS prd_line,
CAST([prd_start_dt] AS DATE),
CAST(LEAD([prd_start_dt]) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
FROM [DataWarehouse].[bronze].[crm_prd_info];

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds'
PRINT '---------------------------------'

----------------------------

SET @start_time = GETDATE();

PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data Into: silver.crm_sales_details';

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
[sls_ord_num],
[sls_prd_key],
[sls_cust_id],
CASE
	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS varchar) AS date)
	END
AS sls_order_dt,
CASE
	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS varchar) AS date)
	END
AS sls_ship_dt,
CASE
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS varchar) AS date)
	END
AS sls_due_dt,

CASE
	WHEN (sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != ABS(sls_quantity) * ABS(sls_price)) THEN ABS(sls_quantity) * ABS(sls_price)
	ELSE sls_sales
	END
AS sls_sales,

ABS([sls_quantity]) AS sls_quantity,

CASE
	WHEN (sls_price IS NULL OR sls_price <= 0) THEN ABS(sls_sales) / ABS(NULLIF(sls_quantity, 0))
	ELSE sls_price
	END
AS sls_price
FROM [DataWarehouse].[bronze].[crm_sales_details];

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds'
PRINT '---------------------------------'

-------------------------------

PRINT '---------------------------------';
PRINT 'Loading ERP Tables Data';
PRINT '---------------------------------';

SET @start_time = GETDATE();

PRINT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting Data Into: silver.erp_cust_az12';

INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)

SELECT
CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE
	WHEN upper(trim(gen)) IN ('F', 'FEMALE') Then 'Female'
	WHEN upper(trim(gen)) IN ('M', 'MALE') Then 'Male'
	ELSE 'n/a'
END AS gen
FROM [DataWarehouse].[bronze].[erp_cust_az12];

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds'
PRINT '---------------------------------'

--------------------------

SET @start_time = GETDATE();

PRINT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Inserting Data Into: silver.erp_loc_a101';

INSERT INTO silver.erp_loc_a101 (
cid,
cntry
)

SELECT 
REPLACE(cid, '-', '') AS cid,
CASE
	WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITEDSTATES') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM [DataWarehouse].[bronze].[erp_loc_a101];

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds'
PRINT '---------------------------------'

---------------------------

SET @start_time = GETDATE();

PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

INSERT INTO silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance
)

SELECT 
[id],
CASE
	WHEN cat IS NOT NULL THEN TRIM(cat)
ELSE 'n/a'
END AS cat,
CASE
	WHEN subcat IS NOT NULL THEN LTRIM(RTRIM(subcat))
ELSE 'n/a'
END AS subcat,
CASE
	WHEN maintenance IS NOT NULL THEN TRIM(maintenance)
ELSE 'n/a'
END AS maintenance
FROM [DataWarehouse].[bronze].[erp_px_cat_g1v2];

SET @end_time = GETDATE();

PRINT '>> Load Duration: ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + 'milliseconds'
PRINT '---------------------------------'

SET @batch_end_time = GETDATE();

PRINT '=================================';
PRINT 'Lodaing Bronze layer is complete';
PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' milliseconds';
PRINT '=================================';

--------------------------

END TRY

BEGIN CATCH

PRINT '=================================';
PRINT 'Error occurred during Loading Silver Layer';
PRINT 'Error Message' + ERROR_MESSAGE();
PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
PRINT '=================================';

END CATCH

END
