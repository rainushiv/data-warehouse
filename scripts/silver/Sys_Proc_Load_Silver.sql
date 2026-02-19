



--CLEAN AND INSERT INTO silver.crm_cust_info

TRUNCATE TABLE silver.crm_cust_info;

insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_martial_status,
cst_gndr,
cst_create_date
)
select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_martial_status)) = 'S' then 'Single'
	when upper(trim(cst_martial_status)) = 'M' then 'Male'	
	else 'n/a'
end cst_martial_status,
case when upper(trim(cst_gndr)) = 'F' then 'Female'
	when upper(trim(cst_gndr)) = 'M' then 'Male'	
	else 'n/a'
end cst_gndr,
cst_create_date 
from(
select *,
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info 
where cst_id is not null
)t where flag_last = 1