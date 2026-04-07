import os
import sys
from uuid import UUID
import time_uuid
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.functions import udf, col, when, count, sum as _sum, round as _round
from pyspark.sql.types import StringType

def create_spark_session():
    """Initializes Spark Session with Cassandra and MySQL connectors."""
    return SparkSession.builder \
        .appName("ETL_Batch_Processing_Warehouse") \
        .config("spark.driver.allowMultipleContexts", "true") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,mysql:mysql-connector-java:8.0.28") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

def process_raw_data(df):
    """Transforms raw TimeUUIDs from Cassandra into human-readable timestamps."""
    @udf(returnType=StringType())
    def to_datetime_str(x):
        if x is None: return None
        return time_uuid.TimeUUID(bytes=UUID(x).bytes).get_datetime().strftime('%Y-%m-%d %H:%M:%S')

    return df.withColumn('ts', to_datetime_str(col('create_time'))).filter(col('ts').isNotNull())

def aggregate_data(df):
    """Performs multi-dimensional aggregation for hourly performance metrics."""
    return df.groupBy(
        sf.date_format('ts', 'yyyy-MM-dd').alias('dates'),
        sf.hour('ts').alias('hours'),
        'job_id', 'publisher_id', 'campaign_id', 'group_id'
    ).agg(
        _round(_sum(when(col('custom_track') == 'click', col('bid')).otherwise(0)), 2).alias('spend_hour'),
        _round(sf.avg(when(col('custom_track') == 'click', col('bid'))), 2).alias('bid_set'),
        count(when(col('custom_track') == 'click', 1)).alias('clicks'),
        count(when(col('custom_track') == 'conversion', 1)).alias('conversion'),
        count(when(col('custom_track') == 'qualified', 1)).alias('qualified_application'),
        count(when(col('custom_track') == 'unqualified', 1)).alias('disqualified_application')
    )

def main():
    """Main ETL orchestration logic: Cassandra -> Spark -> MySQL."""
    spark = create_spark_session()
    
    # 1. Extraction: Load raw events from Cassandra Distributed File System
    print(">>> [INGESTION] Extracting data from Cassandra...")
    raw_data = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='tracking', keyspace='keyspace_name').load()

    # 2. Transformation: Data Cleaning and Aggregation
    processed_data = process_raw_data(raw_data)
    metrics_df = aggregate_data(processed_data)

    # 3. Enrichment: Join with MySQL metadata (Job/Company Dimensions)
    mysql_common_config = {
        "url": "jdbc:mysql://mysql:3306/schema_name", 
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",
        "password": "root"
    }
    
    jobs_df = spark.read.format("jdbc").options(**mysql_common_config) \
        .option("dbtable", "(SELECT id as job_id, company_id FROM job) A").load()

    # 4. Persistence: Write final enriched records to Data Warehouse
    final_df = metrics_df.join(jobs_df, on='job_id', how='left') \
        .withColumn('updated_at', sf.current_timestamp()) \
        .withColumn('sources', sf.lit('Cassandra_Batch'))

    print(">>> [LOAD] Persisting aggregated data to MySQL Warehouse...")
    final_df.write.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://mysql:3306/recruitment_dw") \
        .option("dbtable", "events") \
        .mode("append") \
        .option("user", "root") \
        .option("password", "root") \
        .save()

    print(">>> [FINALIZE] Batch ETL Cycle Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()