# PySpark Code Generated from SAS Translation
# Generated on: 2025-07-31 22:13:08.187175

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("PublicDataProcessing").getOrCreate()

def public(dataset, debtcode, sampledate):
    # Load data (assuming Parquet or database connections for simplicity)
    bdata_accountmapping = spark.read.parquet('bdata_accountmapping.parquet')
    bctl_controlrun = spark.read.parquet('bctl_controlrun.parquet')
    bdata_publicinfo_mapping = spark.read.parquet('bdata_publicinfo_mapping.parquet')
    bdata_publicinfo = spark.read.parquet('bdata_publicinfo.parquet')

    # Step 1: Get controlrunid
    controlrunid = bctl_controlrun.filter(col('currentind') == 1).select('controlrunid').first()['controlrunid']

    # Step 2: Filter account mapping
    accountmapping = bdata_accountmapping.filter(col('currentind') == 1).select('accountnumber', 'personid')

    # Step 3: Prepare sample data
    _sample = dataset.select(debtcode, sampledate).withColumnRenamed(debtcode, 'accountnumber').withColumnRenamed(sampledate, 'sampledate')

    # Step 4: Join sample data with account mapping
    debtcode_pid = _sample.join(accountmapping, on='accountnumber', how='inner')

    # Step 5: Create public mapping
    publicmapping = bdata_publicinfo_mapping.filter(col('currentind') == 1).join(
        bdata_publicinfo, on='PublicInformationID', how='inner'
    )

    # Step 6: Filter public data
    publicdata = publicmapping.join(debtcode_pid, on='PersonID', how='left')
    publicdata = publicdata.filter(col('PublicInformationDate') <= col('sampledate'))

    # Step 7: Sort public data
    window_spec = Window.partitionBy('accountnumber', 'sampledate').orderBy(col('PublicInformationDate').desc())
    publicdata = publicdata.withColumn('row_num', row_number().over(window_spec)).filter(col('row_num') == 1)

    # Step 8: Summarize public data
    publicsummary = publicdata.groupBy('accountnumber', 'sampledate').agg(
        # Perform aggregations based on PublicInformationTypeID (replace with actual logic)
    )

    # Drop unnecessary columns
    columns_to_drop = [
        'PublicInformationDate', 'PublicInformationID', 'PublicInformationTypeID', 'LocationID', 'SettledDate', 'amount'
    ]
    publicsummary = publicsummary.drop(*columns_to_drop)

    return publicsummary

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DropTables").getOrCreate()

# Drop tables by unregistering them from the catalog
for table in ['accountmapping', '_sample', 'debtcode_pid', 'publicmapping', 'publicdata']:
    spark.catalog.dropTempView(table)

# Note: PySpark does not directly support dropping tables in external databases. If the tables are stored in Hive or another external system, you would need to use SQL commands or external tools.