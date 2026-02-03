source(output(
		transaction_id as string,
		customer_id as string,
		category as string,
		item as string,
		price_per_unit as decimal(3,1),
		quantity as decimal(10,0),
		total_spent as decimal(5,1),
		payment_method as string,
		sales_channel as string,
		transaction_date as date,
		discount_applied as integer
	),
	allowSchemaDrift: true,
	validateSchema: false,
	ignoreNoFilesFound: false,
	format: 'parquet',
	fileSystem: 'silver',
	fileName: 'retail_sales') ~> source1
source1 aggregate(groupBy(transaction_date,
		category,
		sales_channel,
		payment_method),
	total_revenue = sum(total_spent),
		total_quantity = sum(quantity),
		total_transactions = count(transaction_id),
		discounted_txns = sum(discount_applied)) ~> aggregate1
source1 select(mapColumn(
		transaction_id,
		customer_id
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select1
select1 aggregate(groupBy(customer_id),
	total_transactions = count(transaction_id),
	partitionBy('hash', 1)) ~> aggregate2
source1 select(mapColumn(
		transaction_id,
		category,
		item
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select2
select2 aggregate(groupBy(category,
		item),
	product_txn_count = count(transaction_id)) ~> aggregate3
select3 derive(year = year(transaction_date),
		day = dayOfMonth(transaction_date),
		day_name = dayOfWeek(transaction_date),
		month = month(transaction_date)) ~> derivedColumn1
source1 select(mapColumn(
		transaction_date
	),
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true) ~> select3
aggregate1 sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	partitionFileNames:['fact_sales_daily'],
	umask: 0222,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> factsales
aggregate2 sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	fileSystem: 'gold',
	compressionCodec: 'none',
	partitionFileNames:['dim_customers'],
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> dimcustomer
aggregate3 sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	fileSystem: 'gold',
	partitionFileNames:['dim_products'],
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> dimproduct
derivedColumn1 sink(allowSchemaDrift: true,
	validateSchema: false,
	format: 'parquet',
	fileSystem: 'gold',
	partitionFileNames:['dim_date'],
	umask: 0022,
	preCommands: [],
	postCommands: [],
	skipDuplicateMapInputs: true,
	skipDuplicateMapOutputs: true,
	partitionBy('hash', 1)) ~> dimdate