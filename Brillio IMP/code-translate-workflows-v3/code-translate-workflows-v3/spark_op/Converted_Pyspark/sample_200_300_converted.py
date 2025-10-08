# PySpark Code Generated from SAS Translation
# Generated on: 2025-07-31 22:14:25.075495

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max, lit, date_add, months_between

spark = SparkSession.builder.appName('SAS to PySpark').getOrCreate()

# Step 1: SQL Join
mb_search_data = spark.read.csv('MB_SearchData.csv', header=True, inferSchema=True)
completed_sample_acc = spark.read.csv('COMPLETEDSAMPLE_ACC.csv', header=True, inferSchema=True)

# Perform the join
ecaps = mb_search_data.join(completed_sample_acc.select('NewID', 'debt_code', 'sampledate'), 
                            mb_search_data.NewID == completed_sample_acc.NewID, 
                            'inner')
ecaps = ecaps.withColumnRenamed('debt_code', 'accountnumber')

# Step 2: Sort
ecaps = ecaps.orderBy(['accountnumber', 'sampledate'])

# Step 3: Drop tables (simulated by not persisting the original tables)

# Step 4: Define macros as functions
def searchvars(type):
    return {
        f'NumSearches_{type}': lit(0),
        f'NumSearchesL12M_{type}': lit(0),
        f'NumSearchesL6M_{type}': lit(0),
        f'NumSearchesL3M_{type}': lit(0),
        f'MostRecentSearch_{type}': lit(None)
    }

def searches(df, type):
    df = df.withColumn(f'NumSearches_{type}', when(col('applicationdate') < col('sampledate'), col(f'NumSearches_{type}') + 1))
    df = df.withColumn(f'NumSearchesL12M_{type}', when((date_add(col('sampledate'), -365) < col('applicationdate')) & (col('applicationdate') < col('sampledate')), col(f'NumSearchesL12M_{type}') + 1))
    df = df.withColumn(f'NumSearchesL6M_{type}', when((date_add(col('sampledate'), -180) < col('applicationdate')) & (col('applicationdate') < col('sampledate')), col(f'NumSearchesL6M_{type}') + 1))
    df = df.withColumn(f'NumSearchesL3M_{type}', when((date_add(col('sampledate'), -90) < col('applicationdate')) & (col('applicationdate') < col('sampledate')), col(f'NumSearchesL3M_{type}') + 1))
    df = df.withColumn(f'MostRecentSearch_{type}', max(col(f'MostRecentSearch_{type}'), col('applicationdate')))
    return df

def searchcalcs(df, type):
    df = df.withColumn(f'MonthsRecentSearch_{type}', when(col(f'MostRecentSearch_{type}').isNotNull(), months_between(col('sampledate'), col(f'MostRecentSearch_{type}'))))
    df = df.withColumn(f'AgeRecentSearch_{type}', when(col(f'MostRecentSearch_{type}').isNull(), lit(-999999)))
    return df

# Step 5: Data processing
out_data = ecaps
out_data = out_data.filter(col('applicationdate') <= col('sampledate'))

out_data = searches(out_data, 'ALL')
out_data = searchcalcs(out_data, 'ALL')

# Step 6: Drop table
# Simulated by not persisting the original table

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, when, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Load data from BDCC.Searches
df_searches = spark.sql('SELECT PersonID, SearchOrgTypeID, SearchHistoryDate, SearchReference, SearchPurposeID FROM BDCC.Searches')

# Create CC_SEARCH_NORM and CC_NONCREDSEARCH_NORM
df_searches = df_searches.withColumn('applicationdate', to_date(col('SearchHistoryDate')))
df_searches = df_searches.withColumn('personID_CC', col('PersonID'))
df_searches = df_searches.withColumn('bureau', lit('C'))

cc_search_norm = df_searches.filter(col('SearchPurposeID') == 12).select('personID_CC', 'applicationdate', 'bureau')
cc_noncredsearch_norm = df_searches.filter(col('SearchPurposeID') != 12).select('personID_CC', 'applicationdate', 'bureau')

# Load data from BDATA.ExCreditApplicationMapping and BDATA.ExCreditApplication
df_experian_searches = spark.sql('''
    SELECT t1.PersonID, t2.ApplicationDate, t2.ApplicationTypeID
    FROM BDATA.ExCreditApplicationMapping t1
    INNER JOIN BDATA.ExCreditApplication t2
    ON t1.ExCreditapplicationID = t2.ExCreditApplicationID
''')

df_experian_searches = df_experian_searches.withColumn('bureau', lit('E'))

# Join with completedsample
df_exp_search_withid = df_experian_searches.join(df_completedsample, on='PersonID', how='inner').select('NEWID', 'ApplicationDate', 'bureau')
df_cc_search_withid = cc_search_norm.join(df_completedsample, df_cc_search_norm['personID_CC'] == df_completedsample['PersonID'], how='inner').select('NEWID', 'applicationdate', 'bureau')

# Append data
df_all_search_append = df_cc_search_withid.union(df_exp_search_withid)

# Sort data
df_sorted = df_all_search_append.orderBy(['NEWID', 'applicationdate'])

# Remove duplicates based on logic
window_spec = Window.partitionBy('NEWID').orderBy('applicationdate')
df_sorted = df_sorted.withColumn('row_num', row_number().over(window_spec))

def remove_duplicates(df):
    return df.filter((col('row_num') == 1) | (col('applicationdate') != col('temp_applicationdate')) | (col('bureau') != col('temp_bureau')))

df_mb_searchdata = remove_duplicates(df_sorted)
