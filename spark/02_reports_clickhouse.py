from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import clickhouse_connect

PG_URL = "jdbc:postgresql://spark_postgres:5432/petshop"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

CH_HOST = "spark_clickhouse"
CH_PORT = 8123
CH_DB   = "reports"

spark = SparkSession.builder \
    .appName("Reports_ClickHouse") \
    .config("spark.jars", "/opt/spark-jobs/jars/postgresql-42.7.3.jar") \
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

ch = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, database=CH_DB)

def write_ch(df, table, ddl):
    ch.command(f"DROP TABLE IF EXISTS {table}")
    ch.command(ddl)
    pdf = df.toPandas()
    ch.insert_df(table, pdf)
    print(f"Written {len(pdf)} rows → ClickHouse:{table}")


top_products = facts.join(
    products,
    (facts.product_name == products.product_name) &
    (facts.product_brand == products.brand),
    "left"
).groupBy(
    facts.product_name, products.brand, products.category
).agg(
    F.sum("sale_quantity").alias("total_units_sold"),
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("sale_quantity").alias("sales_count")
).orderBy(F.desc("total_units_sold")).limit(10)

write_ch(top_products, "report_top_products", """
CREATE TABLE report_top_products (
    product_name     String,
    brand            String,
    category         String,
    total_units_sold Int64,
    total_revenue    Float64,
    sales_count      Int64
) ENGINE = MergeTree() ORDER BY total_units_sold
""")

revenue_by_category = facts.join(
    products,
    (facts.product_name == products.product_name) &
    (facts.product_brand == products.brand),
    "left"
).groupBy(products.category).agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.sum("sale_quantity").alias("total_units_sold"),
    F.count("*").alias("sales_count")
).orderBy(F.desc("total_revenue"))

write_ch(revenue_by_category, "report_revenue_by_category", """
CREATE TABLE report_revenue_by_category (
    category         String,
    total_revenue    Float64,
    total_units_sold Int64,
    sales_count      Int64
) ENGINE = MergeTree() ORDER BY total_revenue
""")

product_ratings = products.select(
    "product_name", "brand", "category", "rating", "reviews", "price"
).orderBy(F.desc("rating"))

write_ch(product_ratings, "report_product_ratings", """
CREATE TABLE report_product_ratings (
    product_name String,
    brand        String,
    category     String,
    rating       Float32,
    reviews      Int32,
    price        Float64
) ENGINE = MergeTree() ORDER BY rating
""")

top_customers = facts.join(
    customers, facts.customer_email == customers.email, "left"
).groupBy(
    customers.email, customers.first_name,
    customers.last_name, customers.country
).agg(
    F.sum("sale_total_price").alias("total_spent"),
    F.count("*").alias("orders_count"),
    F.avg("sale_total_price").alias("avg_order_value")
).orderBy(F.desc("total_spent")).limit(10)

write_ch(top_customers, "report_top_customers", """
CREATE TABLE report_top_customers (
    email           String,
    first_name      String,
    last_name       String,
    country         String,
    total_spent     Float64,
    orders_count    Int64,
    avg_order_value Float64
) ENGINE = MergeTree() ORDER BY total_spent
""")

customers_by_country = facts.join(
    customers, facts.customer_email == customers.email, "left"
).groupBy(customers.country).agg(
    F.countDistinct("customer_email").alias("unique_customers"),
    F.sum("sale_total_price").alias("total_revenue"),
    F.avg("sale_total_price").alias("avg_order_value")
).orderBy(F.desc("total_revenue"))

write_ch(customers_by_country, "report_customers_by_country", """
CREATE TABLE report_customers_by_country (
    country          String,
    unique_customers Int64,
    total_revenue    Float64,
    avg_order_value  Float64
) ENGINE = MergeTree() ORDER BY total_revenue
""")

avg_check = facts.join(
    customers, facts.customer_email == customers.email, "left"
).groupBy(
    customers.email, customers.first_name, customers.last_name
).agg(
    F.avg("sale_total_price").alias("avg_check"),
    F.sum("sale_total_price").alias("total_spent"),
    F.count("*").alias("orders_count")
).orderBy(F.desc("avg_check"))

write_ch(avg_check, "report_customer_avg_check", """
CREATE TABLE report_customer_avg_check (
    email        String,
    first_name   String,
    last_name    String,
    avg_check    Float64,
    total_spent  Float64,
    orders_count Int64
) ENGINE = MergeTree() ORDER BY avg_check
""")

sales_with_date = facts.filter(F.col("sale_date").isNotNull()) \
    .withColumn("year",       F.year("sale_date")) \
    .withColumn("month",      F.month("sale_date")) \
    .withColumn("year_month", F.date_format("sale_date", "yyyy-MM"))

monthly_trends = sales_with_date.groupBy("year", "month", "year_month").agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("*").alias("sales_count"),
    F.avg("sale_total_price").alias("avg_order_value"),
    F.sum("sale_quantity").alias("total_units_sold")
).orderBy("year", "month")

write_ch(monthly_trends, "report_monthly_trends", """
CREATE TABLE report_monthly_trends (
    year             Int32,
    month            Int32,
    year_month       String,
    total_revenue    Float64,
    sales_count      Int64,
    avg_order_value  Float64,
    total_units_sold Int64
) ENGINE = MergeTree() ORDER BY (year, month)
""")

yearly_trends = sales_with_date.groupBy("year").agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("*").alias("sales_count"),
    F.avg("sale_total_price").alias("avg_order_value")
).orderBy("year")

write_ch(yearly_trends, "report_yearly_trends", """
CREATE TABLE report_yearly_trends (
    year            Int32,
    total_revenue   Float64,
    sales_count     Int64,
    avg_order_value Float64
) ENGINE = MergeTree() ORDER BY year
""")

top_stores = facts.join(
    stores, facts.store_email == stores.email, "left"
).groupBy(
    stores.email, stores.store_name, stores.city, stores.country
).agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("*").alias("sales_count"),
    F.avg("sale_total_price").alias("avg_check")
).orderBy(F.desc("total_revenue")).limit(5)

write_ch(top_stores, "report_top_stores", """
CREATE TABLE report_top_stores (
    email         String,
    store_name    String,
    city          String,
    country       String,
    total_revenue Float64,
    sales_count   Int64,
    avg_check     Float64
) ENGINE = MergeTree() ORDER BY total_revenue
""")

sales_by_location = facts.join(
    stores, facts.store_email == stores.email, "left"
).groupBy(stores.city, stores.country).agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("*").alias("sales_count"),
    F.avg("sale_total_price").alias("avg_check")
).orderBy(F.desc("total_revenue"))

write_ch(sales_by_location, "report_sales_by_location", """
CREATE TABLE report_sales_by_location (
    city          String,
    country       String,
    total_revenue Float64,
    sales_count   Int64,
    avg_check     Float64
) ENGINE = MergeTree() ORDER BY total_revenue
""")

top_suppliers = facts.join(
    suppliers, facts.supplier_email == suppliers.email, "left"
).groupBy(
    suppliers.email, suppliers.supplier_name, suppliers.country
).agg(
    F.sum("sale_total_price").alias("total_revenue"),
    F.count("*").alias("sales_count")
).orderBy(F.desc("total_revenue")).limit(5)

write_ch(top_suppliers, "report_top_suppliers", """
CREATE TABLE report_top_suppliers (
    email         String,
    supplier_name String,
    country       String,
    total_revenue Float64,
    sales_count   Int64
) ENGINE = MergeTree() ORDER BY total_revenue
""")

supplier_stats = facts.join(
    suppliers, facts.supplier_email == suppliers.email, "left"
).join(
    products,
    (facts.product_name == products.product_name) &
    (facts.product_brand == products.brand),
    "left"
).groupBy(suppliers.country).agg(
    F.avg(products.price).alias("avg_product_price"),
    F.sum(facts.sale_total_price).alias("total_revenue"),
    F.count("*").alias("sales_count")
).orderBy(F.desc("total_revenue"))

write_ch(supplier_stats, "report_supplier_by_country", """
CREATE TABLE report_supplier_by_country (
    country           String,
    avg_product_price Float64,
    total_revenue     Float64,
    sales_count       Int64
) ENGINE = MergeTree() ORDER BY total_revenue
""")

product_quality = facts.join(
    products,
    (facts.product_name == products.product_name) &
    (facts.product_brand == products.brand),
    "left"
).groupBy(
    products.product_name, products.brand, products.category,
    products.rating, products.reviews
).agg(
    F.sum(facts.sale_quantity).alias("total_units_sold"),
    F.sum(facts.sale_total_price).alias("total_revenue")
).orderBy(F.desc("rating"), F.desc("reviews"))

write_ch(product_quality, "report_product_quality", """
CREATE TABLE report_product_quality (
    product_name     String,
    brand            String,
    category         String,
    rating           Float32,
    reviews          Int32,
    total_units_sold Int64,
    total_revenue    Float64
) ENGINE = MergeTree() ORDER BY rating
""")

print("All 6 reports written to ClickHouse successfully!")
spark.stop()
