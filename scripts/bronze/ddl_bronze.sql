SET GLOBAL local_infile = 1;

CREATE DATABASE datawarehouse;
USE datawarehouse;
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id INT,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_material_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);

DROP TABLE IF EXISTS bronze.cm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key VARCHAR(50),
prd_num VARCHAR(50),
prd_cost INT,
prd_line VARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

DROP TABLE IF EXISTS bronze.cm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid VARCHAR(50),
bdate DATE,
gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id VARCHAR(50),
cat VARCHAR(50),
subcat VARCHAR(50),
maitenance VARCHAR(50)
);
SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'secure_file_priv';


SELECT '>> Truncating crm_cust_info table' AS message;
TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  @cst_id,
  @cst_key,
  @cst_firstname,
  @cst_lastname,
  @cst_material_status,
  @cst_gndr,
  @cst_create_date
)
SET
  cst_id              = NULLIF(TRIM(@cst_id), ''),
  cst_key             = NULLIF(TRIM(@cst_key), ''),
  cst_firstname       = NULLIF(TRIM(@cst_firstname), ''),
  cst_lastname        = NULLIF(TRIM(@cst_lastname), ''),
  cst_material_status = NULLIF(TRIM(@cst_material_status), ''),
  cst_gndr            = NULLIF(TRIM(@cst_gndr), ''),
  cst_create_date =
    CASE
      WHEN TRIM(@cst_create_date) = '' THEN NULL
      WHEN TRIM(@cst_create_date) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(@cst_create_date), '%Y-%m-%d')
      WHEN TRIM(@cst_create_date) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
        THEN STR_TO_DATE(TRIM(@cst_create_date), '%d-%m-%Y')
      ELSE NULL
    END;
    
    
SELECT * FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_cust_info;

TRUNCATE TABLE bronze.crm_prd_info;

LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  @prd_id,
  @prd_key,
  @prd_num,
  @prd_cost,
  @prd_line,
  @prd_start_dt,
  @prd_end_dt
)
SET
  prd_id       = NULLIF(TRIM(@prd_id), ''),
  prd_key      = NULLIF(TRIM(@prd_key), ''),
  prd_num      = NULLIF(TRIM(@prd_num), ''),
  prd_cost     = NULLIF(TRIM(@prd_cost), ''),
  prd_line     = NULLIF(TRIM(@prd_line), ''),
  prd_start_dt =
    CASE
      WHEN TRIM(@prd_start_dt) = '' THEN NULL
      WHEN TRIM(@prd_start_dt) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN STR_TO_DATE(TRIM(@prd_start_dt), '%Y-%m-%d')
      ELSE NULL
    END,
  prd_end_dt =
    CASE
      WHEN TRIM(@prd_end_dt) = '' THEN NULL
      WHEN TRIM(@prd_end_dt) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN STR_TO_DATE(TRIM(@prd_end_dt), '%Y-%m-%d')
      ELSE NULL
    END;


TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
IGNORE 1 ROWS
(
  @sls_ord_num,
  @sls_prd_key,
  @sls_cust_id,
  @sls_order_dt,
  @sls_ship_dt,
  @sls_due_dt,
  @sls_sales,
  @sls_quantity,
  @sls_price
)
SET
  sls_ord_num  = NULLIF(TRIM(@sls_ord_num), ''),
  sls_prd_key  = NULLIF(TRIM(@sls_prd_key), ''),
  sls_cust_id  = NULLIF(TRIM(@sls_cust_id), ''),
  sls_sales    = NULLIF(TRIM(@sls_sales), ''),
  sls_quantity = NULLIF(TRIM(@sls_quantity), ''),
  sls_price    = NULLIF(TRIM(@sls_price), ''),

  sls_order_dt =
    CASE
      WHEN TRIM(@sls_order_dt) REGEXP '^[0-9]{8}$'
        THEN CAST(TRIM(@sls_order_dt) AS UNSIGNED)
      ELSE NULL
    END,

  sls_ship_dt =
    CASE
      WHEN TRIM(@sls_ship_dt) REGEXP '^[0-9]{8}$'
        THEN CAST(TRIM(@sls_ship_dt) AS UNSIGNED)
      ELSE NULL
    END,

  sls_due_dt =
    CASE
      WHEN TRIM(@sls_due_dt) REGEXP '^[0-9]{8}$'
        THEN CAST(TRIM(@sls_due_dt) AS UNSIGNED)
      ELSE NULL
    END;



TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  @cid,
  @cntry
)
SET
  cid   = NULLIF(TRIM(@cid), ''),
  cntry = NULLIF(TRIM(@cntry), '');
  
  
TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  @cid,
  @bdate,
  @gen
)
SET
  cid = NULLIF(TRIM(@cid), ''),
  gen = NULLIF(TRIM(@gen), ''),
  bdate =
    CASE
      WHEN TRIM(@bdate) = '' THEN NULL
      WHEN TRIM(@bdate) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN STR_TO_DATE(TRIM(@bdate), '%Y-%m-%d')
      WHEN TRIM(@bdate) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
        THEN STR_TO_DATE(TRIM(@bdate), '%d-%m-%Y')
      ELSE NULL
    END;



TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA INFILE
'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  @id,
  @cat,
  @subcat,
  @maintenance
)
SET
  id          = NULLIF(TRIM(@id), ''),
  cat         = NULLIF(TRIM(@cat), ''),
  subcat      = NULLIF(TRIM(@subcat), ''),
  maitenance  = NULLIF(TRIM(@maintenance), '');
