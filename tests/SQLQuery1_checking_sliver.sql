/*
* Contains steps for quality checks that were followed while building silver layer tables
* First the data quality was observed in bronze layer tables
* Then after various transformations the data quality was observed in the output of silver tables to ensure consistency of data as per business needs and for appropriate analysis further
*/

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING
count(*) > 1;

SELECT
*
FROM silver.crm_cust_info;

------------- bronze prd_info checks -------------

SELECT
PRD_ID,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING
count(*) > 1;

SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT
*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------------- silver prd_info checks -------------

SELECT
PRD_ID,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING
count(*) > 1;

SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------------- bronze sales_details checks -------------

SELECT
sls_ord_num,
COUNT(*)
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING
count(*) > 1;

SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT
*
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

------------- silver sales_details checks -------------

SELECT
sls_ord_num,
COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_ord_num
HAVING
count(*) > 1;

-- Check of invalid order dates
select
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check sales, qty, price
select
*
FROM silver.crm_sales_details
WHERE
sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_sales <= 0
OR sls_quantity IS NULL OR sls_quantity <= 0
OR sls_price IS NULL OR sls_price <= 0;

SELECT * FROM silver.crm_sales_details

------------- bronze erp_cust_az12 checks -------------

SELECT
cid,
COUNT(*)
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING
count(*) > 1;

SELECT DISTINCT
gen
FROM bronze.erp_cust_az12;

------------- bronze loc_a101 checks -------------
SELECT
cid,
COUNT(*)
FROM bronze.erp_loc_a101
GROUP BY cid
HAVING
count(*) > 1 OR cid IS NULL;

SELECT DISTINCT
cntry,
CASE
	WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
	WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITEDSTATES') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101;

------------- bronze erp_px_cat_g1v2 checks -------------
SELECT
id,
COUNT(*)
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING
count(*) > 1 OR id IS NULL;

SELECT DISTINCT
[subcat]
FROM bronze.erp_px_cat_g1v2;

------------- SILVER erp_px_cat_g1v2 checks -------------

SELECT
*
FROM silver.erp_px_cat_g1v2;
