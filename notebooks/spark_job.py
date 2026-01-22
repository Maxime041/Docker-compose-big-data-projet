import os
import time
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType, TimestampType

def main():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'

    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("high_wind_alert", BooleanType(), True),
        StructField("time", StringType(), True)
    ])

    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "weather_transformed") \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
    parsed = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
    parsed = parsed.withColumn("event_time", col("time").cast(TimestampType()))

    agg = parsed.groupBy(
        window(col("event_time"), "1 minute")
    ).agg(
        avg("temperature").alias("avg_temp_c"),
        count(col("high_wind_alert")).alias("alert_count")
    )

    agg.show(truncate=False)

    client = InsecureClient("http://namenode:9870", user="root")
    
    pandas_df = agg.toPandas()
    csv_content = pandas_df.to_csv(index=False)
    
    hdfs_dir = "/weather_data"
    client.makedirs(hdfs_dir)
    
    file_path = f"{hdfs_dir}/agg_{int(time.time())}.csv"
    
    with client.write(file_path, encoding='utf-8', overwrite=True) as writer:
        writer.write(csv_content)

    spark.stop()

if __name__ == "__main__":
    main()