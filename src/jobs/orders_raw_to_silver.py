from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType
)


def main():
    spark = (
        SparkSession.builder
        .appName("orders_raw_to_silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_path = "data/raw/orders.csv"
    silver_path = "data/silver/orders"

    # Read schema:
    # user_id/product_id come as "65.0" in CSV (because pandas wrote floats due to nulls),
    # so we read them as Double and then cast to int.
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("user_id", DoubleType(), True),
        StructField("product_id", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("order_ts", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_date", StringType(), True),
    ])

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(raw_path)
    )

    print("RAW ROW COUNT:", df.count())
    df.printSchema()

    # --- Debug BEFORE casting/parsing
    print("NULL user_id (raw):", df.filter(F.col("user_id").isNull()).count())
    print("NULL product_id (raw):", df.filter(F.col("product_id").isNull()).count())
    print("NULL order_ts (raw):", df.filter(F.col("order_ts").isNull()).count())
    print("SAMPLE order_ts (raw):", [r["order_ts"] for r in df.select("order_ts").limit(5).collect()])

    # Cast IDs to int (fix for "65.0" strings -> double -> int)
    df = (
        df
        .withColumn("user_id", F.col("user_id").cast("int"))
        .withColumn("product_id", F.col("product_id").cast("int"))
    )

    # Parse timestamp (invalid values -> null)
    df = df.withColumn(
        "order_ts",
        F.to_timestamp(F.trim(F.col("order_ts")), "yyyy-MM-dd HH:mm:ss")
    )

    # Normalize event_date to yyyy-MM-dd (keep as string for partitioning)
    # If event_date missing, derive from order_ts
    df = df.withColumn(
        "event_date",
        F.coalesce(
            F.to_date(F.col("event_date"), "yyyy-MM-dd"),
            F.to_date(F.col("order_ts"))
        ).cast("string")
    )

    # --- Debug AFTER parse/cast
    print("NULL user_id (casted):", df.filter(F.col("user_id").isNull()).count())
    print("NULL product_id (casted):", df.filter(F.col("product_id").isNull()).count())
    print("NULL order_ts (parsed):", df.filter(F.col("order_ts").isNull()).count())
    print("NULL event_date (final):", df.filter(F.col("event_date").isNull()).count())

    # Cleaning rules (basic quality gates)
    df_clean = (
        df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("order_ts").isNotNull())
        .filter(F.col("event_date").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
    )

    print("AFTER CLEANING ROW COUNT:", df_clean.count())

    # Dedup by order_id
    df_dedup = df_clean.dropDuplicates(["order_id"])
    print("AFTER DEDUP ROW COUNT:", df_dedup.count())

    # Write Silver (partitioned)
    (
        df_dedup
        .write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(silver_path)
    )
    print("✅ Silver orders written to:", silver_path)

    spark.stop()


if __name__ == "__main__":
    main()
