from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum, max, min, current_timestamp, 
    date_format, hour, to_date, lit, when, round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime


def main():

    # ==========================================================
    # STEP 5 : ANALYTICS ZONE - KPI AND BUSINESS STRUCTURING
    # ==========================================================

    # 1. Spark Session initialization
    spark = SparkSession.builder \
        .appName("SmartCityAnalyticsZone") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("STEP 5 : ANALYTICS ZONE - BUSINESS STRUCTURING")
    print("=" * 80)

    # 2. Read processed datasets from HDFS
    processed_base_path = "hdfs://namenode:8020/data/processed/traffic"
    analytics_base_path = "hdfs://namenode:8020/data/analytics/traffic"

    print(f"\n>>> Reading processed data from: {processed_base_path}")

    try:
        traffic_by_zone_df = spark.read.parquet(f"{processed_base_path}/traffic_by_zone")
        speed_by_road_df = spark.read.parquet(f"{processed_base_path}/speed_by_road")
        congestion_alerts_df = spark.read.parquet(f"{processed_base_path}/congestion_alerts")

        print(">>> Processed data loaded successfully")
        print(f"   - Traffic by zone: {traffic_by_zone_df.count()} rows")
        print(f"   - Speed by road: {speed_by_road_df.count()} rows")
        print(f"   - Congestion alerts: {congestion_alerts_df.count()} rows")

    except Exception as e:
        print(f"ERROR while reading processed data: {e}")
        spark.stop()
        return

    # 3. KPI creation and business enrichment
    print("\n>>> Creating analytical KPIs...")

    # A. Traffic KPIs by zone
    analytics_traffic_by_zone = traffic_by_zone_df \
        .withColumn("avg_vehicle_count", round(col("avg_vehicle_count"), 2)) \
        .withColumn("avg_occupancy_rate", round(col("avg_occupancy_rate"), 2)) \
        .withColumn("traffic_level",
                    when(col("avg_occupancy_rate") < 20, "LOW")
                    .when(col("avg_occupancy_rate") < 50, "MODERATE")
                    .when(col("avg_occupancy_rate") < 80, "HIGH")
                    .otherwise("CRITICAL")) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("zone"),
            col("avg_vehicle_count").alias("vehicle_count_avg"),
            col("avg_occupancy_rate").alias("occupancy_rate_avg"),
            col("traffic_level"),
            col("processed_date")
        )

    print(">>> Traffic KPIs created")
    analytics_traffic_by_zone.show(truncate=False)

    # B. Speed KPIs by road
    analytics_speed_by_road = speed_by_road_df \
        .withColumn("avg_speed", round(col("avg_speed"), 2)) \
        .withColumn("speed_category",
                    when(col("avg_speed") > 80, "FAST")
                    .when(col("avg_speed") > 50, "NORMAL")
                    .when(col("avg_speed") > 30, "SLOW")
                    .otherwise("VERY_SLOW")) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("road_id"),
            col("road_type"),
            col("avg_speed").alias("speed_avg_kmh"),
            col("speed_category"),
            col("processed_date")
        )

    print("\n>>> Speed KPIs created")
    analytics_speed_by_road.show(truncate=False)

    # C. Congestion KPIs
    analytics_congestion = congestion_alerts_df \
        .withColumn("global_congestion_rate", round(col("global_congestion_rate"), 2)) \
        .withColumn("congestion_priority",
                    when(col("global_congestion_rate") > 80, "URGENT")
                    .when(col("global_congestion_rate") > 50, "HIGH")
                    .when(col("global_congestion_rate") > 30, "MODERATE")
                    .otherwise("LOW")) \
        .withColumn("processed_date", current_timestamp()) \
        .select(
            col("zone"),
            col("global_congestion_rate").alias("congestion_rate_pct"),
            col("measurements_count"),
            col("status"),
            col("congestion_priority"),
            col("processed_date")
        )

    print("\n>>> Congestion KPIs created")
    analytics_congestion.show(truncate=False)

    # D. Consolidated mobility summary
    print("\n>>> Creating consolidated mobility view...")

    mobility_summary = analytics_traffic_by_zone \
        .join(
            analytics_congestion.select("zone", "congestion_rate_pct", "congestion_priority"),
            on="zone",
            how="outer"
        ) \
        .withColumn("processed_date", current_timestamp()) \
        .withColumn("alert_status",
                    when(col("congestion_rate_pct").isNotNull(), "ALERT")
                    .otherwise("NORMAL"))

    print(">>> Mobility summary view created")
    mobility_summary.show(truncate=False)

    # 4. Save into Analytics zone with optimized partitioning
    print(f"\n>>> Saving analytics data into: {analytics_base_path}")
    print(">>> Format: Parquet with Snappy compression")

    try:
        run_date_expr = date_format(current_timestamp(), "yyyy-MM-dd")

        analytics_traffic_by_zone_d = analytics_traffic_by_zone.withColumn("run_date", run_date_expr)
        analytics_speed_by_road_d = analytics_speed_by_road.withColumn("run_date", run_date_expr)
        analytics_congestion_d = analytics_congestion.withColumn("run_date", run_date_expr)
        mobility_summary_d = mobility_summary.withColumn("run_date", run_date_expr)

        analytics_traffic_by_zone_d.write \
            .mode("append") \
            .partitionBy("zone", "run_date") \
            .parquet(f"{analytics_base_path}/traffic_by_zone")
        print("   ✓ Traffic by zone saved")

        analytics_speed_by_road_d.write \
            .mode("append") \
            .partitionBy("road_type", "run_date") \
            .parquet(f"{analytics_base_path}/speed_by_road")
        print("   ✓ Speed by road saved")

        analytics_congestion_d.write \
            .mode("append") \
            .partitionBy("run_date") \
            .parquet(f"{analytics_base_path}/congestion_alerts")
        print("   ✓ Congestion alerts saved")

        mobility_summary_d.write \
            .mode("append") \
            .partitionBy("zone", "run_date") \
            .parquet(f"{analytics_base_path}/mobility_summary")
        print("   ✓ Mobility summary saved")

    except Exception as e:
        print(f"ERROR while saving analytics data: {e}")
        spark.stop()
        return

    # 5. Validation
    print("\n>>> Validating analytics files...")

    try:
        verification_df = spark.read.parquet(f"{analytics_base_path}/traffic_by_zone")
        print("   ✓ Parquet files validated")
        print(f"   ✓ Number of zones: {verification_df.select('zone').distinct().count()}")
        print("   ✓ Schema:")
        verification_df.printSchema()

        print("\n>>> Data preview:")
        verification_df.show(5, truncate=False)

    except Exception as e:
        print(f"ERROR during validation: {e}")

    # 6. Summary
    print("\n" + "=" * 80)
    print("ANALYTICS ZONE SUMMARY")
    print("=" * 80)
    print("""
    ✓ Parquet format chosen for:
      - High compression efficiency
      - Column-based storage
      - Partitioning support
      - Cross-platform compatibility

    ✓ Partitioning strategy:
      - By zone
      - By road type
      - By execution date

    ✓ KPIs created:
      - Traffic level
      - Speed category
      - Congestion priority
      - Consolidated mobility indicators

    ✓ Optimizations:
      - Snappy compression
      - Typed schema
      - Execution timestamps
    """)

    print(">>> STEP 5 COMPLETED SUCCESSFULLY!")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()
