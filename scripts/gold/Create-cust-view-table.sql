
create view gold.dim_customers as
SELECT 
row_number() over (order by ci.cst_id) as customer_key,
ci.cst_id as customer_id, 
ci.cst_key as customer_number,
ci,cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_martial_status as marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
	else coalesce(ca.gen, 'n/a')
end as gender,
ca.bdate as birthday,
ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la 
on ci.cst_key = la.cid 




SELECT distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
	else coalesce(ca.gen, 'n/a')
end as new_gen
FROM silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la 
on ci.cst_key = la.cid 
order by 1,2
