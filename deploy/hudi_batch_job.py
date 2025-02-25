from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HudiBatchJob") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .getOrCreate()

# Define Hudi options
hudiOptions = {
    'hoodie.table.name': 'po_hudi_table',
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'creation_date',
    'hoodie.datasource.write.precombine.field': 'last_update_time',
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.table': 'po_hudi_table',
    'hoodie.datasource.hive_sync.partition_fields': 'creation_date',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
}

# Write initial data to Hudi dataset
inputDF = spark.createDataFrame(
    [
        ("100", "2015-01-01", "2015-01-01T13:51:39.340396Z"),
        ("101", "2015-01-01", "2015-01-01T12:14:58.597216Z"),
        ("102", "2015-01-01", "2015-01-01T13:51:40.417052Z"),
        ("103", "2015-01-01", "2015-01-01T13:51:40.519832Z"),
        ("104", "2015-01-02", "2015-01-01T12:15:00.512679Z"),
        ("105", "2015-01-02", "2015-01-01T13:51:42.248818Z"),
    ],
    ["id", "creation_date", "last_update_time"]
)

inputDF.write \
    .format('org.apache.hudi') \
    .option('hoodie.datasource.write.operation', 'insert') \
    .options(**hudiOptions) \
    .mode('overwrite') \
    .save(HUDI_DATASET_PATH)

# Perform an upsert operation
updateDF = inputDF.limit(1).withColumn('creation_date', lit('new_value'))

updateDF.write \
    .format('org.apache.hudi') \
    .option('hoodie.datasource.write.operation', 'upsert') \
    .options(**hudiOptions) \
    .mode('append') \
    .save(HUDI_DATASET_PATH)

# Perform a delete operation
updateDF.write \
    .format('org.apache.hudi') \
    .option('hoodie.datasource.write.operation', 'upsert') \
    .option('hoodie.datasource.write.payload.class', 'org.apache.hudi.common.model.EmptyHoodieRecordPayload') \
    .options(**hudiOptions) \
    .mode('append') \
    .save(HUDI_DATASET_PATH)

# Read from the Hudi dataset
snapshotQueryDF = spark.read \
    .format('org.apache.hudi') \
    .load(HUDI_DATASET_PATH + '/*/*')

snapshotQueryDF.show()