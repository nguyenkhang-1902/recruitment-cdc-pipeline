from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, when, count, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark():
    return SparkSession.builder \
        .appName("ETL_With_Kafka") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.28") \
        .getOrCreate()

def main():
    spark = create_spark()
    
    # --- Cấu hình MySQL (ĐÃ ĐỔI SANG HOST DOCKER) ---
    # Sửa localhost thành mysql
    dw_url = "jdbc:mysql://mysql:3306/recruitment_dw"
    src_url = "jdbc:mysql://mysql:3306/schema_name" # Tên DB chứa bảng job của bạn
    db_user = "root"
    db_pass = "root"

    # --- 1. Đọc dữ liệu Danh mục (Jobs) ---
    jobs_df = spark.read.format("jdbc") \
        .options(url=src_url, driver="com.mysql.cj.jdbc.Driver", dbtable="job", user=db_user, password=db_pass).load() \
        .select(col("id").alias("job_id"), "company_id", "campaign_id", "group_id")

    # --- 2. Định nghĩa Schema ---
    kafka_schema = StructType([
        StructField("job_id", IntegerType(), True),
        StructField("custom_track", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("publisher_id", IntegerType(), True),
        StructField("bid", StringType(), True)
    ])

    # --- 3. Đọc Stream từ Kafka (HOST broker:29092) ---
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "tracking_events") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # --- 4. Transform ---
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
        .select(sf.from_json("value", kafka_schema).alias("data")) \
        .select("data.*") \
        .withColumn("ts_timestamp", sf.to_timestamp("ts")) \
        .withColumn("dates", sf.date_format("ts_timestamp", "yyyy-MM-dd")) \
        .withColumn("hours", sf.hour("ts_timestamp"))

    # --- 5. Join Stream với Batch ---
    enriched_stream = parsed_stream.join(jobs_df, on="job_id", how="left")

    # --- 6. Aggregate ---
    final_agg = enriched_stream.groupBy("dates", "hours", "job_id", "publisher_id", "campaign_id", "group_id", "company_id") \
        .agg(
            sf.round(sf.sum(when(col("custom_track") == "click", col("bid").cast("float")).otherwise(0)), 2).alias("spend_hour"),
            count(when(col("custom_track") == "click", 1)).alias("clicks"),
            count(when(col("custom_track") == "conversion", 1)).alias("conversion"),
            count(when(col("custom_track") == "qualified", 1)).alias("qualified_application"),
            count(when(col("custom_track") == "unqualified", 1)).alias("disqualified_application")
        ) \
        .withColumn("updated_at", sf.current_timestamp()) \
        .withColumn("sources", lit("Kafka_Streaming"))

    # --- 7. Sink: Ghi vào MySQL ---
    def write_to_mysql(df, epoch_id):
        # Ta dùng phương thức này để ghi vào MySQL vì JDBC chưa hỗ trợ trực tiếp writeStream tốt như Batch
        df.write.format("jdbc") \
            .option("url", dw_url) \
            .option("dbtable", "events") \
            .option("user", db_user).option("password", db_pass) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append").save()

    checkpoint_path = "/opt/airflow/scripts/processing/checkpoints/stream_etl"
    
    query = final_agg.writeStream \
    .queryName("shibe_realtime_query") \
    .foreachBatch(write_to_mysql) \
    .outputMode("update") \
    .option("checkpointLocation", checkpoint_path) \
    .option("failOnDataLoss", "false") \
    .trigger(processingTime='30 seconds') \
    .start()

    print(">>> [BEAR MODE] REAL-TIME PIPELINE IS RUNNING...")
    query.awaitTermination()

if __name__ == "__main__":
    main()