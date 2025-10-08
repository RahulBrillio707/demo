# PySpark Code Generated from SAS Translation
# Generated on: 2025-07-31 22:25:49.614210

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, current_date, lit
from pyspark.sql.types import DateType

# Initialize Spark session
spark = SparkSession.builder.appName("BureauDataBlock").getOrCreate()

def BureauDataBlock(dataset, debtcode, sampledate):
    # Simulate the %bureaucalcs macro (if any logic is defined here, it should be implemented)
    # Placeholder for bureaucalcs logic

    # Simulate the controlrunid extraction
    controlrunid = bctl_controlrun.filter(col('currentind') == 1).select('controlrunid').collect()[0][0]

    # Create accountmapping dataframe
    accountmapping = bdata_accountmapping.filter(col('currentind') == 1).select('accountnumber', 'personid')

    # Create _sample dataframe
    _sample = dataset.select(debtcode, sampledate)
    _sample = _sample.filter(col(sampledate) > date_add(current_date(), -36 * 30))
    _sample = _sample.withColumnRenamed(debtcode, 'accountnumber').withColumnRenamed(sampledate, 'sampledate')

    # Create debtcode_pid dataframe
    debtcode_pid = _sample.join(accountmapping, on='accountnumber', how='inner')

    # Create caisinfo dataframe
    caisinfo = bdata_vw_CreditAccountHeaderCurrent.filter(
        col('CAISSourceCode').isNull() | (~col('CAISSourceCode').isin([320, 592]))
    )
    caisinfo = caisinfo.join(debtcode_pid, on='personid', how='inner')

    # Create credaccdistinct dataframe
    credaccdistinct = caisinfo.select('personid', 'CreditAccountHeaderID', 'Opendate', 'CloseDate', 'DefaultDate', 'SourceUpdateDate').distinct()

    # Create status dataframe
    status = bdata_status.filter(col('priority') < 9990)

    # Create credacchist dataframe
    credacchist = credaccdistinct.join(bdata_creditaccount, on='CreditAccountHeaderID', how='inner')
    credacchist = credacchist.filter(col('controlrunid') == lit(controlrunid))
    for i in range(1, 37):
        credacchist = credacchist.join(status, col(f'statusid{i}') == col('statusid'), how='left')
    credacchist = credacchist.join(debtcode_pid, on='personid', how='inner')

    # Create cc dataframe
    cc = caisinfo.filter(col('AccountTypeCode') == '5').select('CreditAccountHeaderID', 'personid', 'AccountTypeCode')

    return credacchist, cc

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, date_format, lit, expr

# Initialize Spark session
spark = SparkSession.builder.appName("SAS to PySpark Translation").getOrCreate()

# Load data (assuming caisinfocombined is a Spark DataFrame)
caisinfocombined = spark.read.format("csv").option("header", "true").load("path_to_data")

# Define constants
HIST = 36

# Process data
def process_data(df):
    df = df.withColumn("lastupdatedate", date_format(col("SourceUpdateDate"), "yyyy-MM-dd"))
    df = df.withColumn("Month", expr("greatest(months_between(current_date(), sampledate), 1)"))
    df = df.withColumn("RetroMonth", when(col("lastupdatedate").isNotNull(),
                                           expr("Month - greatest(months_between(current_date(), lastupdatedate), 1) + 1"))
                                 .otherwise(lit(1)))

    df = df.withColumn("AccountTypeCode", when(expr("date(opendate) > sampledate"), lit("")))

    # Initialize arrays (PySpark doesn't support arrays directly, so use columns)
    for i in range(1, min(HIST, 36) + 1):
        retro_index = expr(f"RetroMonth + {i} - 1")
        df = df.withColumn(f"bal{i}", when(retro_index > 0,
                                            col(f"balance{retro_index}"))
                                 .otherwise(when(col("balance1") == 0, lit(0)).otherwise(lit(None))))
        df = df.withColumn(f"status{i}", when(retro_index > 0,
                                               col(f"statuscodepriority{retro_index}"))
                                 .otherwise(when(col("balance1") == 0, col("statuscodepriority1")).otherwise(lit(None))))
        df = df.withColumn(f"cc_bal{i}", when(retro_index > 0,
                                               col(f"cc_balance{retro_index}"))
                                 .otherwise(when(col("balance1") == 0, lit(0)).otherwise(lit(None))))
        df = df.withColumn(f"cc_amt{i}", when(retro_index > 0,
                                               col(f"cc_amount{retro_index}"))
                                 .otherwise(when(col("balance1") == 0, lit(0)).otherwise(lit(None))))

    # Assign accgroup
    df = df.withColumn("accgroup", when(col("closedate").isNull() & col("defaultdate").isNull(), lit("active"))
                                 .when(col("defaultdate").isNull() & (col("closedate") <= col("sampledate")), lit("settled"))
                                 .when((col("defaultdate") <= col("sampledate")) & col("closedate").isNull(), lit("default"))
                                 .when((col("defaultdate") < col("closedate")) & (col("closedate") <= col("sampledate")), lit("settled default"))
                                 .when(col("defaultdate").isNull() & (col("closedate") > col("sampledate")), lit("settled was active"))
                                 .when((col("defaultdate") > col("sampledate")) & col("closedate").isNull(), lit("default was active"))
                                 .when((col("defaultdate") > col("sampledate")) & col("closedate").isNotNull(), lit("settled default was active"))
                                 .when((col("defaultdate") <= col("sampledate")) & (col("sampledate") < col("closedate")), lit("settled default was default")))

    return df

work_cais1 = process_data(caisinfocombined)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = SparkSession.builder.appName("SAS to PySpark Translation").getOrCreate()

# Placeholder functions for macros
def createvars(var_type):
    pass

def calculatedvars(var_type):
    pass

def creditcardvars():
    pass

def creditcardcalcs():
    pass

def summarycalcs(var_type):
    pass

def ccsummary():
    pass

# Load the data into a Spark DataFrame
cais1 = spark.read.csv("path_to_cais1.csv", header=True, inferSchema=True)

# Sort the data by 'accountnumber' and 'sampledate'
cais1 = cais1.orderBy(['accountnumber', 'sampledate'])

# Define a function to process each group
from pyspark.sql import Window
from pyspark.sql.functions import first, last

windowSpec = Window.partitionBy("sampledate").orderBy("accountnumber")

# Placeholder for processing logic
# Note: PySpark does not support row-wise operations as in pandas, so the logic needs to be adapted

# Perform summary calculations
# Placeholder for summary calculations logic

# Drop unnecessary columns
columns_to_drop = [
    'creditaccountheaderid', 'opendate', 'closedate', 'defaultdate', 'statuscode_current',
    'currentbalance', 'defaultbalance', 'specialinstructioncode', 'specialinstructionstartdate',
    'creditlimit', 'accounttypecode', 'accounttypedesc', 'caissourcecode',
    'balance1', 'balance36', 'statuscodepriority1', 'statuscodepriority36',
    'bal1', 'bal36', 'status1', 'status36', 'lastupdatedate', 'month', 'retromonth',
    'MonthClosed', 'i', 'f', 'sourceupdatedate', 'accgroup', 'cc_bal', 'cc_am', '_temp', 'specinstrpriority'
]

cais1 = cais1.drop(*columns_to_drop)

# Save the processed data
cais1.write.csv("path_to_output.csv", header=True)

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SAS to PySpark Translation").getOrCreate()

# Define the output variable
out_pid = 'pids'
print(out_pid)

# Load the data from the source DataFrame
work_debtcode_pid = spark.createDataFrame([], schema=None)  # Replace with actual data loading logic
out_pid_df = work_debtcode_pid

# Drop tables (simulated as removing tables in a database or clearing variables)
tables_to_drop = [
    'accountmapping', '_sample', 'caisinfo', 'credaccdistinct', 'credacchist',
    'debtcode_pid', 'cc', 'apacscombine', 'voters', 'voterid'
]

# Simulate dropping tables (e.g., database operations)
for table in tables_to_drop:
    # Replace with actual logic to drop tables in a database
    print(f"Dropping table: {table}")

# Note: In PySpark, dropping tables would typically involve database operations or clearing temporary views.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, lag, max as spark_max, expr
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SAS to PySpark").getOrCreate()

# Step 1: apacscombine equivalent
cc = spark.read.csv('cc.csv', header=True)
ccp = spark.read.csv('creditcardpayment.csv', header=True)

apacscombine = cc.join(ccp, on='creditaccountheaderid', how='inner')
for i in range(1, 37):
    apacscombine = apacscombine.withColumn(f'cc_balance{i}', col(f'previousbalance{i}'))
for j in range(1, 37):
    apacscombine = apacscombine.withColumn(f'cc_amount{j}', col(f'amount{j}'))

# Step 2: voters equivalent
voter_mapping = spark.read.csv('VoterMapping.csv', header=True)
voter = spark.read.csv('Voter.csv', header=True)

debtcode_pid = spark.read.csv('debtcode_pid.csv', header=True)

voters = voter_mapping.join(voter, on='VoterID', how='inner').filter(col('currentind') == 1)

voterid = voters.join(debtcode_pid, on='personid', how='inner')
voterid = voterid.filter(col('DateRegistered') <= col('sampledate'))

window_spec = Window.partitionBy('accountnumber', 'sampledate').orderBy(col('DateRegistered').desc())

voterid = voterid.withColumn('V_CurrentlyRegistered', lit(0))
voterid = voterid.withColumn('V_TimeAtAddress', lit(0))
voterid = voterid.withColumn('V_NumAddL3M', lit(0))
voterid = voterid.withColumn('V_NumAddL6M', lit(0))
voterid = voterid.withColumn('V_NumAddL12M', lit(0))
voterid = voterid.withColumn('V_NumAddL24M', lit(0))
voterid = voterid.withColumn('V_NumAddL48M', lit(0))

voterid = voterid.withColumn('laglocation', lag('locationid').over(window_spec))

voterid = voterid.withColumn('V_CurrentlyRegistered', when(col('DateLeft').isNull() | (col('DateLeft') > col('sampledate')), 1).otherwise(0))
voterid = voterid.withColumn('V_TimeAtAddress', when(col('DateLeft').isNull() | (col('DateLeft') > col('sampledate')), expr("datediff(sampledate, DateRegistered) / 30")).otherwise(0))

# Incremental counts for address changes
voterid = voterid.withColumn('V_NumAddL3M', when(spark_max(col('DateRegistered'), col('DateLeft')) > expr("add_months(sampledate, -3)"), col('V_NumAddL3M') + 1).otherwise(col('V_NumAddL3M')))
voterid = voterid.withColumn('V_NumAddL6M', when(spark_max(col('DateRegistered'), col('DateLeft')) > expr("add_months(sampledate, -6)"), col('V_NumAddL6M') + 1).otherwise(col('V_NumAddL6M')))
voterid = voterid.withColumn('V_NumAddL12M', when(spark_max(col('DateRegistered'), col('DateLeft')) > expr("add_months(sampledate, -12)"), col('V_NumAddL12M') + 1).otherwise(col('V_NumAddL12M')))
voterid = voterid.withColumn('V_NumAddL24M', when(spark_max(col('DateRegistered'), col('DateLeft')) > expr("add_months(sampledate, -24)"), col('V_NumAddL24M') + 1).otherwise(col('V_NumAddL24M')))
voterid = voterid.withColumn('V_NumAddL48M', when(spark_max(col('DateRegistered'), col('DateLeft')) > expr("add_months(sampledate, -48)"), col('V_NumAddL48M') + 1).otherwise(col('V_NumAddL48M')))

# Step 3: caisinfocombined equivalent
caisinfo = spark.read.csv('caisinfo.csv', header=True)
credacchist = spark.read.csv('credacchist.csv', header=True)

caisinfocombined = caisinfo.join(credacchist, on=['personid', 'CreditAccountHeaderID'], how='inner')
caisinfocombined = caisinfocombined.join(apacscombine, on=['personid', 'CreditAccountHeaderID'], how='left')

for i in range(1, 37):
    caisinfocombined = caisinfocombined.withColumn(f'balance{i}', col(f'balance{i}'))
for j in range(1, 37):
    caisinfocombined = caisinfocombined.withColumn(f'StatusCodePriority{j}', col(f'StatusCodePriority{j}'))
for k in range(1, 37):
    caisinfocombined = caisinfocombined.withColumn(f'cc_balance{k}', col(f'cc_balance{k}'))
for l in range(1, 37):
    caisinfocombined = caisinfocombined.withColumn(f'cc_amount{l}', col(f'cc_amount{l}'))

caisinfocombined = caisinfocombined.orderBy(['accountnumber', 'sampledate'])