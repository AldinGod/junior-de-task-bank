from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def main():
    spark = (
        SparkSession.builder
        .appName("products_raw_to_silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_path = "data/raw/products.csv"
    silver_path = "data/silver/products"

    schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("product_name", StringType(), True),
    ])

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(raw_path)
    )

    print("RAW PRODUCTS ROW COUNT:", df.count())
    df.printSchema()

    df_clean = (
        df
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("brand", F.trim(F.col("brand")))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .filter(F.col("product_id").isNotNull())
    )

    # fill nulls (dim tables usually prefer defaults)
    df_clean = (
        df_clean
        .withColumn("category", F.coalesce(F.col("category"), F.lit("unknown")))
        .withColumn("brand", F.coalesce(F.col("brand"), F.lit("unknown")))
        .withColumn("product_name", F.coalesce(F.col("product_name"), F.lit("unknown")))
    )

    print("AFTER CLEANING PRODUCTS ROW COUNT:", df_clean.count())

    # dedup by product_id (just in case)
    df_dedup = df_clean.dropDuplicates(["product_id"])
    print("AFTER DEDUP PRODUCTS ROW COUNT:", df_dedup.count())

    (
        df_dedup
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print("✅ Silver products written to:", silver_path)

    spark.stop()


if __name__ == "__main__":
    main()
