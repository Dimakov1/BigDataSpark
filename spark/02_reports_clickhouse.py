from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://spark_postgres:5432/petshop"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

CH_URL = "jdbc:ch://spark-clickhouse:8123/reports"
CH_PROPS = {
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

spark = SparkSession.builder \
    .appName("Reports_ClickHouse") \
    .config(
        "spark.jars",
        "/opt/spark-jobs/jars/postgresql-42.7.3.jar,/opt/spark-jobs/jars/clickhouse-jdbc-0.9.8-all.jar"
    ) \
    .config(
        "spark.driver.extraClassPath",
        "/opt/spark-jobs/jars/postgresql-42.7.3.jar:/opt/spark-jobs/jars/clickhouse-jdbc-0.9.8-all.jar"
    ) \
    .config(
        "spark.executor.extraClassPath",
        "/opt/spark-jobs/jars/postgresql-42.7.3.jar:/opt/spark-jobs/jars/clickhouse-jdbc-0.9.8-all.jar"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def read_pg(table):
    return spark.read.jdbc(PG_URL, table, properties=PG_PROPS)

facts     = read_pg("fact_sales")
products  = read_pg("dim_product")
customers = read_pg("dim_customer")
stores    = read_pg("dim_store")
suppliers = read_pg("dim_supplier")

facts.cache()


def write_ch(df, table):
    (
        df.write
        .format("jdbc")
        .option("url", CH_URL)
        .option("dbtable", table)
        .option("user", CH_PROPS["user"])
        .option("password", CH_PROPS["password"])
        .option("driver", CH_PROPS["driver"])
        .mode("append")
        .save()
    )
    print(f"Written {df.count()} rows → ClickHouse:{table}")


facts_products = facts.join(
    products,
    (facts.product_name == products.product_name) &
    (facts.product_brand == products.brand),
    "left"
)

product_sales = facts_products.groupBy(
    facts.product_name.alias("product_name"),
    products.brand.alias("brand"),
    products.category.alias("category")
).agg(
    F.sum(facts.sale_quantity).cast("long").alias("total_units_sold"),
    F.sum(facts.sale_total_price).cast("double").alias("total_revenue"),
    F.count("*").cast("long").alias("sales_count")
)

product_ratings = products.select(
    products.product_name.alias("product_name"),
    products.brand.alias("brand"),
    products.category.alias("category"),
    products.rating.cast("double").alias("avg_rating"),
    products.reviews.cast("long").alias("reviews_count")
)

category_revenue = facts_products.groupBy(products.category.alias("category")).agg(
    F.sum(facts.sale_total_price).cast("double").alias("category_total_revenue"),
    F.sum(facts.sale_quantity).cast("long").alias("category_total_units_sold"),
    F.count("*").cast("long").alias("category_sales_count")
)

report_products = product_sales \
    .join(product_ratings, ["product_name", "brand", "category"], "left") \
    .join(category_revenue, "category", "left") \
    .withColumn("product_sales_rank", F.row_number().over(Window.orderBy(F.desc("total_units_sold"))))

write_ch(report_products, "report_products")

customer_sales = facts.join(customers, facts.customer_email == customers.email, "left").groupBy(
    customers.email.alias("email"),
    customers.first_name.alias("first_name"),
    customers.last_name.alias("last_name"),
    customers.country.alias("country")
).agg(
    F.sum(facts.sale_total_price).cast("double").alias("total_spent"),
    F.count("*").cast("long").alias("orders_count"),
    F.avg(facts.sale_total_price).cast("double").alias("avg_check")
)

customers_by_country = facts.join(customers, facts.customer_email == customers.email, "left").groupBy(
    customers.country.alias("country")
).agg(
    F.countDistinct(facts.customer_email).cast("long").alias("country_unique_customers"),
    F.sum(facts.sale_total_price).cast("double").alias("country_total_revenue"),
    F.avg(facts.sale_total_price).cast("double").alias("country_avg_order_value")
)

report_customers = customer_sales \
    .join(customers_by_country, "country", "left") \
    .withColumn("customer_spending_rank", F.row_number().over(Window.orderBy(F.desc("total_spent"))))

write_ch(report_customers, "report_customers")

sales_with_date = facts.filter(F.col("sale_date").isNotNull()) \
    .withColumn("year", F.year("sale_date")) \
    .withColumn("month", F.month("sale_date")) \
    .withColumn("period", F.date_format("sale_date", "yyyy-MM"))

monthly = sales_with_date.groupBy("year", "month", "period").agg(
    F.sum("sale_total_price").cast("double").alias("total_revenue"),
    F.count("*").cast("long").alias("sales_count"),
    F.avg("sale_total_price").cast("double").alias("avg_order_value"),
    F.sum("sale_quantity").cast("long").alias("total_units_sold")
).withColumn("period_type", F.lit("month"))

yearly = sales_with_date.groupBy("year").agg(
    F.sum("sale_total_price").cast("double").alias("year_total_revenue"),
    F.count("*").cast("long").alias("year_sales_count"),
    F.avg("sale_total_price").cast("double").alias("year_avg_order_value")
)

report_time = monthly.join(yearly, "year", "left") \
    .withColumn("previous_period_revenue", F.lag("total_revenue").over(Window.orderBy("year", "month"))) \
    .withColumn("revenue_diff_vs_previous_period", F.col("total_revenue") - F.col("previous_period_revenue")) \
    .withColumn(
        "revenue_pct_diff_vs_previous_period",
        F.when(F.col("previous_period_revenue").isNull() | (F.col("previous_period_revenue") == 0), None)
         .otherwise((F.col("total_revenue") - F.col("previous_period_revenue")) / F.col("previous_period_revenue") * 100)
         .cast("double")
    )

write_ch(report_time, "report_time")

store_sales = facts.join(stores, facts.store_email == stores.email, "left").groupBy(
    stores.email.alias("email"),
    stores.store_name.alias("store_name"),
    stores.city.alias("city"),
    stores.country.alias("country")
).agg(
    F.sum(facts.sale_total_price).cast("double").alias("total_revenue"),
    F.count("*").cast("long").alias("sales_count"),
    F.avg(facts.sale_total_price).cast("double").alias("avg_check")
)

store_location = facts.join(stores, facts.store_email == stores.email, "left").groupBy(
    stores.city.alias("city"),
    stores.country.alias("country")
).agg(
    F.sum(facts.sale_total_price).cast("double").alias("location_total_revenue"),
    F.count("*").cast("long").alias("location_sales_count"),
    F.avg(facts.sale_total_price).cast("double").alias("location_avg_check")
)

report_stores = store_sales \
    .join(store_location, ["city", "country"], "left") \
    .withColumn("store_revenue_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))

write_ch(report_stores, "report_stores")

supplier_sales = facts_products.join(suppliers, facts.supplier_email == suppliers.email, "left").groupBy(
    suppliers.email.alias("email"),
    suppliers.supplier_name.alias("supplier_name"),
    suppliers.country.alias("country")
).agg(
    F.sum(facts.sale_total_price).cast("double").alias("total_revenue"),
    F.count("*").cast("long").alias("sales_count"),
    F.avg(products.price).cast("double").alias("avg_product_price")
)

supplier_country = facts_products.join(suppliers, facts.supplier_email == suppliers.email, "left").groupBy(
    suppliers.country.alias("country")
).agg(
    F.sum(facts.sale_total_price).cast("double").alias("country_total_revenue"),
    F.count("*").cast("long").alias("country_sales_count")
)

report_suppliers = supplier_sales \
    .join(supplier_country, "country", "left") \
    .withColumn("supplier_revenue_rank", F.row_number().over(Window.orderBy(F.desc("total_revenue"))))

write_ch(report_suppliers, "report_suppliers")

quality_base = product_sales.join(product_ratings, ["product_name", "brand", "category"], "left")
quality_corr = quality_base.agg(
    F.corr(F.col("avg_rating"), F.col("total_units_sold")).cast("double").alias("rating_sales_correlation")
)

report_quality = quality_base.crossJoin(quality_corr) \
    .withColumn("rating_rank_desc", F.row_number().over(Window.orderBy(F.desc("avg_rating")))) \
    .withColumn("rating_rank_asc", F.row_number().over(Window.orderBy(F.asc("avg_rating")))) \
    .withColumn("reviews_rank", F.row_number().over(Window.orderBy(F.desc("reviews_count"))))

write_ch(report_quality, "report_quality")

print("All 6 ClickHouse report tables written successfully!")
spark.stop()
