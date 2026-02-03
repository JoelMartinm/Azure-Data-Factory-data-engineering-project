source(output(
		transaction_id as string,
		customer_id as string,
		category as string,
		item as string,
		price_per_unit as decimal(3,1),
		quantity as decimal(10,0),
		total_spent as decimal(4,1),
		payment_method as string,
		location as string,
		transaction_date as date,
		discount_applied as boolean
	),
	allowSchemaDrift: true,
	validateSchema: true,
	inferDriftedColumnTypes: true,
	ignoreNoFilesFound: false,
	format: 'parquet',
	fileSystem: 'bronze',
	fileName: 'retailsales2') ~> source1
select1 filter(!isNull(transaction_id)
&& !isNull(customer_id)
&& quantity > 0
&& price_per_unit >= 0
&& total_spent >= 0) ~> filter1
filter1 derive(discount_applied = iif(discount_applied, 1, 0),
		payment_method = lower(trim(payment_method)),
		item = initCap(trim(item)),
		total_spent = round(total_spent, 2),
		sales_channel = lower(replace(trim(sales_channel), '-', ' '))) ~> derivedColumn1
source1 select(mapColumn(
		transaction_id,
		customer_id,
		category,
		item,
		price_per_unit,
		quantity,
		total_spent,
		payment_method,
		sales_channel = location,
		transaction_date,
		discount_applied
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
derivedColumn1 sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	partitionFileNames:['retail_sales'],
	umask: 0222,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> sink1