

create or alter procedure bronze.load_bronze as 
begin
-- ensures error handling
  begin try
  declare @start_time datetime, @end_time datetime;
  set @start_time = GETDATE();
truncate table bronze.crm_cust_info;
	bulk insert bronze.crm_cust_info
	from 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> load duration for cust_info: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';

	--select count(*)  from bronze.crm_cust_info
	 set @start_time = GETDATE();
	truncate table bronze.crm_prd_info;
	bulk insert bronze.crm_prd_info
	from 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> load duration for prd_info: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';


	--select count(*)  from bronze.crm_prd_info	

	 set @start_time = GETDATE()
	truncate table bronze.crm_sales_details;
	bulk insert bronze.crm_sales_details
	from 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> load duration for sales_details: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';

	--select *  from bronze.crm_sales_details

	set @start_time = GETDATE()
	truncate table bronze.erp_cust_az12;
	bulk insert bronze.erp_cust_az12
	from 'D:\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> load duration for cust az12: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';
	--select *  from bronze.erp_cust_az12

	set @start_time = GETDATE()
	truncate table bronze.erp_loc_a101;
	bulk insert bronze.erp_loc_a101
	from 'D:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	set @end_time = GETDATE();
	print '>> load duration for loc_a101: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';

	--select *  from bronze.erp_loc_a101
	set @start_time = GETDATE()
	truncate table bronze.erp_px_cat_g1v2;
	bulk insert bronze.erp_px_cat_g1v2
	from 'D:\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
	end try
	begin catch
	print'=================================';
	print'Error occured during loading bronze layer';
	print 'Error message' + cast(error_message() as nvarchar);
	print 'Error message' + cast(error_number()as nvarchar);
	print 'Error message' + cast(error_state()as nvarchar);

	end catch
End
	set @end_time = GETDATE();
	print '>> load duration for px_cat_g1v2: '+ cast(datediff(second, @start_time,@end_time) as nvarchar) + 'seconds';



--use DataWarehouse
exec bronze.load_bronze