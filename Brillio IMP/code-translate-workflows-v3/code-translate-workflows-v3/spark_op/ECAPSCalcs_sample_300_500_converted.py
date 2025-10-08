# PySpark Code Generated from SAS Translation
# Generated on: 2025-07-31 22:15:39.586086

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def searchvars(type):
    return {
        f'NumSearches_{type}': 0,
        f'NumSearchesL12M_{type}': 0,
        f'NumSearchesL6M_{type}': 0,
        f'NumSearchesL3M_{type}': 0,
        f'MostRecentSearch_{type}': None,
        f'hurn_retain_{type}': '',
        f'NumAddsSearched_{type}': 0
    }

def searches(df, type):
    df = df.withColumn(f'NumSearches_{type}', F.when(df['applicationdate'] < df['sampledate'], F.col(f'NumSearches_{type}') + 1).otherwise(F.col(f'NumSearches_{type}')))
    df = df.withColumn(f'NumSearchesL12M_{type}', F.when((df['applicationdate'] > F.date_sub(df['sampledate'], 365)) & (df['applicationdate'] < df['sampledate']), F.col(f'NumSearchesL12M_{type}') + 1).otherwise(F.col(f'NumSearchesL12M_{type}')))
    df = df.withColumn(f'NumSearchesL6M_{type}', F.when((df['applicationdate'] > F.date_sub(df['sampledate'], 180)) & (df['applicationdate'] < df['sampledate']), F.col(f'NumSearchesL6M_{type}') + 1).otherwise(F.col(f'NumSearchesL6M_{type}')))
    df = df.withColumn(f'NumSearchesL3M_{type}', F.when((df['applicationdate'] > F.date_sub(df['sampledate'], 90)) & (df['applicationdate'] < df['sampledate']), F.col(f'NumSearchesL3M_{type}') + 1).otherwise(F.col(f'NumSearchesL3M_{type}')))

    window_spec = Window.partitionBy(type).orderBy(F.desc('applicationdate'))
    df = df.withColumn(f'MostRecentSearch_{type}', F.max('applicationdate').over(window_spec))

    df = df.withColumn(f'NumAddsSearched_{type}', F.when(~F.array_contains(F.split(F.col(f'hurn_retain_{type}'), '#'), F.col('locationid').cast('string')), F.col(f'NumAddsSearched_{type}') + 1).otherwise(F.col(f'NumAddsSearched_{type}')))
    df = df.withColumn(f'hurn_retain_{type}', F.when(~F.array_contains(F.split(F.col(f'hurn_retain_{type}'), '#'), F.col('locationid').cast('string')), F.concat(F.col(f'hurn_retain_{type}'), F.lit('#'), F.col('locationid').cast('string'))).otherwise(F.col(f'hurn_retain_{type}')))
    return df

def searchcalcs(df, type):
    df = df.withColumn(f'AgeRecentSearch_{type}', F.when(F.col(f'MostRecentSearch_{type}').isNotNull(), F.months_between(F.col('sampledate'), F.col(f'MostRecentSearch_{type}')).cast('int')).otherwise(-999999))
    df = df.drop(f'hurn_retain_{type}', f'MostRecentSearch_{type}')
    return df

def calcs(df, months):
    df = df.withColumn(f'MaritalStatus_F_{months}', F.lit(None))
    df = df.withColumn(f'MaritalStatus_L_{months}', F.lit(None))
    # Repeat similar logic for other columns

    df = df.withColumn(f'MaritalStatus_F_{months}', F.when((df['applicationdate'] >= F.date_sub(df['sampledate'], months * 30)) & (df['applicationdate'] <= df['sampledate']), df['MaritalStatusID']).otherwise(F.col(f'MaritalStatus_F_{months}')))
    df = df.withColumn(f'MaritalStatus_L_{months}', F.when((df['applicationdate'] >= F.date_sub(df['sampledate'], months * 30)) & (df['applicationdate'] <= df['sampledate']), df['MaritalStatusID']).otherwise(F.col(f'MaritalStatus_L_{months}')))
    # Repeat similar logic for other columns
    return df

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("AppTypeDesc").getOrCreate()

def calcs_app_type(app_type):
    for months in [3, 6, 9, 12, 15, 18, 24]:
        app_type_desc(months, app_type)

def app_type_desc(months, app_type):
    # Assuming `work_out_data` is a Spark DataFrame containing the data from `work.&out_data`
    # Assuming `BDATA` tables are loaded as Spark DataFrames: `accommodation_type`, `employment_status`, `employment_type`, `marital_status`
    
    description_table = work_out_data
    description_table = description_table.join(accommodation_type, 
                                               col(f'AccommodationType_L_{months}_{app_type}') == col('AccommodationTypeID'), 
                                               'left')
    description_table = description_table.join(employment_status, 
                                               col(f'EmploymentStatus_L_{months}_{app_type}') == col('EmploymentStatusID'), 
                                               'left')
    description_table = description_table.join(employment_type, 
                                               col(f'EmploymentType_L_{months}_{app_type}') == col('EmploymentTypeID'), 
                                               'left')
    description_table = description_table.join(marital_status, 
                                               col(f'MaritalStatus_L_{months}_{app_type}') == col('MaritalStatusID'), 
                                               'left')
    description_table = description_table.join(accommodation_type, 
                                               col(f'AccommodationType_F_{months}_{app_type}') == col('AccommodationTypeID'), 
                                               'left')
    description_table = description_table.join(employment_status, 
                                               col(f'EmploymentStatus_F_{months}_{app_type}') == col('EmploymentStatusID'), 
                                               'left')
    description_table = description_table.join(employment_type, 
                                               col(f'EmploymentType_F_{months}_{app_type}') == col('EmploymentTypeID'), 
                                               'left')
    description_table = description_table.join(marital_status, 
                                               col(f'MaritalStatus_F_{months}_{app_type}') == col('MaritalStatusID'), 
                                               'left')
    
    description_table = description_table.orderBy(['AccountNumber', 'SampleDate'])
    
    # Save the resulting DataFrame to a new variable or file
    globals()[f'description_{months}_{app_type}'] = description_table

def drop_app(months, app_type):
    columns_to_drop = [
        f'AccommodationType_F_{months}_{app_type}', f'EmploymentStatus_F_{months}_{app_type}',
        f'EmploymentType_F_{months}_{app_type}', f'MaritalStatus_F_{months}_{app_type}',
        f'AccommodationType_L_{months}_{app_type}', f'EmploymentStatus_L_{months}_{app_type}',
        f'EmploymentType_L_{months}_{app_type}', f'MaritalStatus_L_{months}_{app_type}'
    ]
    work_out_data = work_out_data.drop(*columns_to_drop)

def drop_app_type(app_type):
    for months in [3, 6, 9, 12, 15, 18, 24]:
        drop_app(months, app_type)

def drop(months):
    columns_to_drop = [
        f'AccommodationType_F_{months}', f'EmploymentStatus_F_{months}',
        f'EmploymentType_F_{months}', f'MaritalStatus_F_{months}',
        f'AccommodationType_L_{months}', f'EmploymentStatus_L_{months}',
        f'EmploymentType_L_{months}', f'MaritalStatus_L_{months}'
    ]
    work_out_data = work_out_data.drop(*columns_to_drop)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, first

def ecapsdesc(months, out_data, bdata):
    # Perform the equivalent SQL operation using PySpark
    description = out_data
    description = description.join(bdata['AccommodationType'], out_data[f'AccommodationType_L_{months}'] == bdata['AccommodationTypeID'], 'left')
    description = description.join(bdata['EmploymentStatus'], out_data[f'EmploymentStatus_L_{months}'] == bdata['EmploymentStatusID'], 'left')
    description = description.join(bdata['EmploymentType'], out_data[f'EmploymentType_L_{months}'] == bdata['EmploymentTypeID'], 'left')
    description = description.join(bdata['MaritalStatus'], out_data[f'MaritalStatus_L_{months}'] == bdata['MaritalStatusID'], 'left')
    description = description.join(bdata['AccommodationType'], out_data[f'AccommodationType_F_{months}'] == bdata['AccommodationTypeID'], 'left')
    description = description.join(bdata['EmploymentStatus'], out_data[f'EmploymentStatus_F_{months}'] == bdata['EmploymentStatusID'], 'left')
    description = description.join(bdata['EmploymentType'], out_data[f'EmploymentType_F_{months}'] == bdata['EmploymentTypeID'], 'left')
    description = description.join(bdata['MaritalStatus'], out_data[f'MaritalStatus_F_{months}'] == bdata['MaritalStatusID'], 'left')

    description = description.orderBy(['AccountNumber', 'SampleDate'])
    return description

def app_type(months, app_type, data):
    # Initialize columns for the first sample date
    data = data.withColumn(
        f'MaritalStatus_F_{months}_{app_type}',
        when(first('SampleDate').over(Window.partitionBy('SampleDate')), lit(None))
    )

    # Update columns based on conditions
    condition = (
        (col('AppCode') == lit(app_type)) &
        (col('ApplicationDate') >= col('SampleDate') - expr(f'INTERVAL {months} MONTH')) &
        (col('ApplicationDate') <= col('SampleDate'))
    )

    for col_name in ['MaritalStatusID', 'Dependents', 'AccommodationTypeID', 'EmploymentStatusID', 'EmploymentTypeID', 'GrossIncome', 'TimeEmployment', 'TimeAddress', 'TimeWithBank']:
        data = data.withColumn(
            f'{col_name}_F_{months}_{app_type}',
            when(condition & col(col_name).isNotNull(), col(col_name))
        )
        data = data.withColumn(
            f'{col_name}_L_{months}_{app_type}',
            when(condition & col(f'{col_name}_L_{months}_{app_type}').isNull(), col(col_name))
        )

    return data