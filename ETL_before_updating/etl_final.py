import os
import sys
from uuid import UUID
import time_uuid
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
from pyspark.sql.functions import udf, col, when, count, sum as _sum, round as _round, lit
from pyspark.sql.types import StringType

def create_spark():
    return SparkSession.builder \
        .appName("CDC_Final_Pipeline") \
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,mysql:mysql-connector-java:8.0.28") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .getOrCreate()

def get_mysql_latest_time(spark, url, user, password):
    try:
        # Truy vấn mốc thời gian lớn nhất từ Warehouse
        sql = "(select max(updated_at) as max_ts from events) data"
        mysql_time_df = spark.read.format('jdbc') \
            .options(url=url, driver="com.mysql.cj.jdbc.Driver", dbtable=sql, user=user, password=password).load()
        
        raw_time = mysql_time_df.take(1)[0][0]
        if raw_time is None:
            return '1998-01-01 00:00:00'
        return pd.to_datetime(raw_time).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return '1998-01-01 00:00:00'

def main():
    spark = create_spark()
    
    # Cấu hình kết nối
    dw_url = "jdbc:mysql://localhost:3306/recruitment_dw"
    src_url = "jdbc:mysql://localhost:3306/schema_name"
    db_user = "root"
    db_pass = "root"

    # 1. Lấy Watermark (Mốc thời gian cuối cùng đã xử lý)
    mysql_time = get_mysql_latest_time(spark, dw_url, db_user, db_pass)
    print(f">>> Mốc thời gian cuối trong Warehouse: {mysql_time}")

    # 2. Đọc dữ liệu MỚI từ Cassandra
    print(">>> Đang lấy dữ liệu delta từ Cassandra...")
    raw_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table='tracking', keyspace='keyspace_name').load() \
        .where(col('ts') > mysql_time) # Chỉ lấy những gì mới hơn mốc MySQL

    if raw_df.count() == 0:
        print(">>> Không có dữ liệu mới. Pipeline tạm dừng.")
        spark.stop()
        return

    # 3. Transform: Xử lý và Aggregate
    # Lưu ý: Cột 'ts' của shibe trong Cassandra đã là String/Datetime nên không cần UDF TimeUUID nữa nếu đã chuẩn hóa
    df = raw_df.cache()

    print(">>> Đang tính toán các chỉ số Hourly Aggregation...")
    final_agg = df.groupBy(
        sf.date_format('ts', 'yyyy-MM-dd').alias('dates'),
        sf.hour('ts').alias('hours'),
        'job_id', 'publisher_id', 'campaign_id', 'group_id'
    ).agg(
        _round(_sum(when(col('custom_track') == 'click', col('bid')).otherwise(0)), 2).alias('spend_hour'),
        count(when(col('custom_track') == 'click', 1)).alias('clicks'),
        count(when(col('custom_track') == 'conversion', 1)).alias('conversion'),
        count(when(col('custom_track') == 'qualified', 1)).alias('qualified_application'),
        count(when(col('custom_track') == 'unqualified', 1)).alias('disqualified_application')
    )

    # 4. Enrich: Join với bảng Job từ MySQL Source để lấy company_id
    print(">>> Đang làm giàu dữ liệu (Enrichment) từ MySQL Source...")
    jobs_df = spark.read.format("jdbc") \
        .options(url=src_url, driver="com.mysql.cj.jdbc.Driver", dbtable="job", user=db_user, password=db_pass).load() \
        .select(col("id").alias("job_id"), "company_id")

    final_data = final_agg.join(jobs_df, on="job_id", how="left") \
        .withColumn('updated_at', sf.current_timestamp()) \
        .withColumn('sources', lit('Cassandra'))

    # 5. Load: Ghi vào Warehouse
    print(">>> Đang ghi dữ liệu vào recruitment_dw.events...")
    final_data.write.format("jdbc") \
        .option("url", dw_url) \
        .option("dbtable", "events") \
        .option("user", db_user).option("password", db_pass) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append").save()

    print(f">>> ETL hoàn tất! Đã xử lý {final_data.count()} nhóm dữ liệu.")
    spark.stop()

if __name__ == "__main__":
    main()