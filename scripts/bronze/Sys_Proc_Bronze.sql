--call bronze.load_bronze()

--LOGIC FOR LOADING BRONZE TABLES
SHOW data_directory;


create or replace procedure bronze.load_bronze() 
language plpgsql
as $$
declare 
	err_msg text; 

begin

	RAISE NOTICE '=========================================================================================';

	RAISE NOTICE 'Loading bronze layer';
	
	RAISE NOTICE '=========================================================================================';
	
	RAISE NOTICE '-----------------------------------------------------------------------------------------';
	RAISE NOTICE 'Loading crm tables';
	RAISE NOTICE '-----------------------------------------------------------------------------------------';

	RAISE NOTICE '>> Truncate table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE '>> Inserting into table: bronze.crm_cust_info';

	COPY bronze.crm_cust_info
	FROM '/Users/shivu/postgres_data/Datawarehouse_dataset/cust_info.csv'
	DELIMITER ','
	CSV HEADER;

	RAISE NOTICE '>> Truncate table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	RAISE NOTICE '>> Inserting into table: bronze.crm_prd_info';

	COPY bronze.crm_prd_info
	FROM '/Users/shivu/Documents/My-Projects/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	DELIMITER ','
	CSV HEADER;

	RAISE NOTICE '>> Truncate table: bronze.crm_sales_details';
	
	TRUNCATE TABLE bronze.crm_sales_details;
	RAISE NOTICE '>> Inserting into table: bronze.crm_sales_details';

	COPY bronze.crm_sales_details
	FROM '/Users/shivu/Documents/My-Projects/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	DELIMITER ','
	CSV HEADER;


	RAISE NOTICE '-----------------------------------------------------------------------------------------';
	RAISE NOTICE 'Loading erp tables';
	RAISE NOTICE '-----------------------------------------------------------------------------------------';

	RAISE NOTICE '>> Truncate table: bronze.erp_cust_az12';
	
	TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE NOTICE '>> Inserting into table: bronze.erp_cust_az12';

	COPY bronze.erp_cust_az12
	FROM '/Users/shivu/Documents/My-Projects/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	DELIMITER ','
	CSV HEADER;
	
	RAISE NOTICE '>> Truncate table: bronze.erp_loc_a101';

	TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE NOTICE '>> Insert into table: bronze.erp_loc_a101';

	COPY bronze.erp_loc_a101
	FROM '/Users/shivu/Documents/My-Projects/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	DELIMITER ','
	CSV HEADER;
	
	RAISE NOTICE '>> Truncate table: bronze.erp_px_cat_g1v2';

	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE NOTICE '>> Insert into table: bronze.erp_px_cat_g1v2';

	COPY bronze.erp_px_cat_g1v2
	FROM '/Users/shivu/Documents/My-Projects/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	DELIMITER ','
	CSV HEADER;
		begin 
		EXCEPTION
			when others then
		       GET STACKED DIAGNOSTICS err_msg = MESSAGE_TEXT;
	        	RAISE NOTICE 'Error occurred: %', err_msg;
		end;
		RAISE NOTICE '>> Success';

end;
$$;
