# PySpark Code Generated from SAS Translation
# Generated on: 2025-07-31 22:11:46.046814

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, mean as _mean, count, lit, current_date

# Initialize Spark session
spark = SparkSession.builder.appName("SAS to PySpark Translation").getOrCreate()

# Define threshold
seg_threshold = 5000

# Load raw data
customers_raw = spark.read.csv('raw_customer_data.csv', header=True, inferSchema=True)

# Data cleaning and transformation
customers_raw = customers_raw.filter(col('customer_id').isNotNull())
customers_raw = customers_raw.withColumn('region', when(col('region').isNull(), 'UNKNOWN').otherwise(col('region')))

customers_raw = customers_raw.withColumn('total_spend', 
    _sum([col('spend_cat1'), col('spend_cat2'), col('spend_cat3')])
)
customers_raw = customers_raw.withColumn('total_spend', when(col('total_spend') < 0, 0).otherwise(col('total_spend')))

customers_raw = customers_raw.withColumn('spend_segment', 
    when(col('total_spend') > seg_threshold, 'HIGH')
    .when(col('total_spend') > (seg_threshold / 2), 'MEDIUM')
    .otherwise('LOW')
)

# Aggregation
segment_summary = customers_raw.groupBy('region', 'spend_segment').agg(
    count('customer_id').alias('segment_size'),
    _sum('total_spend').alias('total_segment_revenue'),
    _mean('total_spend').alias('avg_customer_spend')
)

# Highlight column
segment_summary = segment_summary.withColumn('highlight', 
    when(col('total_segment_revenue') > 100000, 'YES').otherwise('NO')
)

# Export function
def format_export(input_df, output_name, file_format):
    input_df = input_df.withColumn('report_date', current_date())
    if file_format == 'xlsx':
        input_df.toPandas().to_excel(f"{output_name}.{file_format}", index=False)
    elif file_format == 'csv':
        input_df.write.csv(f"{output_name}.{file_format}", header=True, mode='overwrite')
    else:
        raise ValueError("Unsupported file format")

# Export the segment_summary
def main():
    format_export(segment_summary, 'segment_analysis', 'xlsx')

if __name__ == "__main__":
    main()