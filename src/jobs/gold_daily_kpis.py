from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("gold_daily_kpis")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    orders_path = "data/silver/orders"
    users_path = "data/silver/users"
    products_path = "data/silver/products"
    gold_path = "data/gold/daily_kpis"

    # ------------------
    # Read silver tables
    # ------------------
    orders = spark.read.parquet(orders_path)
    users = spark.read.parquet(users_path)
    products = spark.read.parquet(products_path)

    print("ORDERS:", orders.count())
    print("USERS:", users.count())
    print("PRODUCTS:", products.count())

    # ------------------
    # Join fact + dims
    # ------------------
    orders_enriched = (
        orders
        .join(users, on="user_id", how="left")
        .join(products, on="product_id", how="left")
    )

    print("AFTER JOIN ROW COUNT:", orders_enriched.count())

    # ------------------
    # Business metrics
    # ------------------
    orders_enriched = orders_enriched.withColumn(
        "order_value",
        F.col("quantity") * F.col("unit_price")
    )

    daily_kpis = (
        orders_enriched
        .groupBy("event_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_value").alias("total_revenue"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg("order_value").alias("avg_order_value"),
        )
        .orderBy("event_date")
    )

    print("GOLD ROW COUNT:", daily_kpis.count())
    daily_kpis.show(10, truncate=False)

    # ------------------
    # Write Gold
    # ------------------
    (
        daily_kpis
        .write
        .mode("overwrite")
        .parquet(gold_path)
    )

    print("✅ Gold daily KPIs written to:", gold_path)

    spark.stop()


if __name__ == "__main__":
    main()
