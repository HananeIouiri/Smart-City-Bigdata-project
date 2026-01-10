from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, when, current_timestamp, date_format

def main():

    print(">>> Starting Spark Raw → Processed job")

    spark = SparkSession.builder \
        .appName("SmartCityRawToProcessed") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ---------------- Read Raw Data ----------------
    raw_path = "hdfs://namenode:8020/data/raw/traffic"

    try:
        df = spark.read.json(raw_path)
        print(">>> Raw data loaded successfully")
        df.printSchema()
    except Exception as e:
        print(f">>> Error reading raw data: {e}")
        return

    # ---------------- Aggregations ----------------

    # Traffic by zone
    traffic_by_zone = df.groupBy("zone").agg(
        avg("vehicle_count").alias("avg_vehicle_count"),
        avg("occupancy_rate").alias("avg_occupancy_rate")
    )

    # Speed by road
    speed_by_road = df.groupBy("road_id", "road_type").agg(
        avg("average_speed").alias("avg_speed")
    )

    # Congestion level
    congestion = df.groupBy("zone").agg(
        avg("occupancy_rate").alias("global_congestion_rate"),
        count("*").alias("measurements_count")
    ).withColumn(
        "status",
        when(col("global_congestion_rate") > 80, "CRITICAL")
        .when(col("global_congestion_rate") > 50, "HIGH")
        .when(col("global_congestion_rate") > 30, "MODERATE")
        .otherwise("LOW")
    )

    # ---------------- Write Processed Zone ----------------

    processed_path = "hdfs://namenode:8020/data/processed/traffic"
    run_date = date_format(current_timestamp(), "yyyy-MM-dd")

    traffic_by_zone.withColumn("run_date", run_date) \
        .write.mode("append").partitionBy("run_date") \
        .parquet(f"{processed_path}/traffic_by_zone")

    speed_by_road.withColumn("run_date", run_date) \
        .write.mode("append").partitionBy("run_date") \
        .parquet(f"{processed_path}/speed_by_road")

    congestion.withColumn("run_date", run_date) \
        .write.mode("append").partitionBy("run_date") \
        .parquet(f"{processed_path}/congestion_alerts")

    print(">>> Processed data written successfully")

    spark.stop()

if __name__ == "__main__":
    main()
