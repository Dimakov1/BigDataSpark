from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PG_URL = "jdbc:postgresql://spark_postgres:5432/petshop"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("ETL_to_Star") \
    .config("spark.jars", "/opt/spark-jobs/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = spark.read.jdbc(PG_URL, "mock_data", properties=PG_PROPS)
raw.cache()
print(f"Loaded mock_data: {raw.count()} rows")

def write_pg(df, table):
    df.write.jdbc(PG_URL, table, mode="append", properties=PG_PROPS)
    print(f"Written {df.count()} rows → {table}")

customers = raw.select(
    F.trim(F.col("customer_first_name")).alias("first_name"),
    F.trim(F.col("customer_last_name")).alias("last_name"),
    F.col("customer_age").alias("age"),
    F.lower(F.trim(F.col("customer_email"))).alias("email"),
    F.trim(F.col("customer_country")).alias("country"),
    F.trim(F.col("customer_postal_code")).alias("postal_code"),
    F.lower(F.trim(F.col("customer_pet_type"))).alias("pet_type"),
    F.trim(F.col("customer_pet_name")).alias("pet_name"),
    F.trim(F.col("customer_pet_breed")).alias("pet_breed"),
).dropDuplicates(["email"])
write_pg(customers, "dim_customer")

sellers = raw.select(
    F.trim(F.col("seller_first_name")).alias("first_name"),
    F.trim(F.col("seller_last_name")).alias("last_name"),
    F.lower(F.trim(F.col("seller_email"))).alias("email"),
    F.trim(F.col("seller_country")).alias("country"),
    F.trim(F.col("seller_postal_code")).alias("postal_code"),
).dropDuplicates(["email"])
write_pg(sellers, "dim_seller")

suppliers = raw.select(
    F.trim(F.col("supplier_name")).alias("supplier_name"),
    F.trim(F.col("supplier_contact")).alias("contact_name"),
    F.lower(F.trim(F.col("supplier_email"))).alias("email"),
    F.trim(F.col("supplier_phone")).alias("phone"),
    F.trim(F.col("supplier_address")).alias("address"),
    F.trim(F.col("supplier_city")).alias("city"),
    F.trim(F.col("supplier_country")).alias("country"),
).dropDuplicates(["email"])
write_pg(suppliers, "dim_supplier")

stores = raw.select(
    F.trim(F.col("store_name")).alias("store_name"),
    F.trim(F.col("store_location")).alias("location"),
    F.trim(F.col("store_city")).alias("city"),
    F.trim(F.col("store_state")).alias("state"),
    F.trim(F.col("store_country")).alias("country"),
    F.trim(F.col("store_phone")).alias("phone"),
    F.lower(F.trim(F.col("store_email"))).alias("email"),
).dropDuplicates(["email"])
write_pg(stores, "dim_store")

products = raw.select(
    F.trim(F.col("product_name")).alias("product_name"),
    F.trim(F.col("product_category")).alias("category"),
    F.trim(F.col("product_brand")).alias("brand"),
    F.col("product_price").alias("price"),
    F.col("product_weight").alias("weight"),
    F.trim(F.col("product_color")).alias("color"),
    F.trim(F.col("product_size")).alias("size"),
    F.trim(F.col("product_material")).alias("material"),
    F.col("product_rating").alias("rating"),
    F.col("product_reviews").alias("reviews"),
    F.trim(F.col("pet_category")).alias("pet_category"),
).dropDuplicates(["product_name", "brand"])
write_pg(products, "dim_product")

facts = raw.select(
    F.to_date(F.col("sale_date"), "M/d/yyyy").alias("sale_date"),
    F.lower(F.trim(F.col("customer_email"))).alias("customer_email"),
    F.lower(F.trim(F.col("seller_email"))).alias("seller_email"),
    F.trim(F.col("product_name")).alias("product_name"),
    F.trim(F.col("product_brand")).alias("product_brand"),
    F.lower(F.trim(F.col("store_email"))).alias("store_email"),
    F.lower(F.trim(F.col("supplier_email"))).alias("supplier_email"),
    F.col("sale_quantity"),
    F.col("sale_total_price"),
)
write_pg(facts, "fact_sales")

print("ETL to Star schema completed successfully!")
spark.stop()
