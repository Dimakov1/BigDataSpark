from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import psycopg2

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
    df.write.jdbc(PG_URL, table, mode="overwrite", properties=PG_PROPS)
    print(f"Written {df.count()} rows → {table}")

conn = psycopg2.connect(
    host="spark_postgres", port=5432,
    dbname="petshop", user="postgres", password="postgres"
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS dim_product_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_product_brand (
    brand_id   SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_pet_type (
    pet_type_id   SERIAL PRIMARY KEY,
    pet_type_name VARCHAR(50) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_pet_breed (
    pet_breed_id   SERIAL PRIMARY KEY,
    pet_breed_name VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_store_city (
    store_city_id SERIAL PRIMARY KEY,
    city_name     VARCHAR(100),
    state_name    VARCHAR(100),
    country_name  VARCHAR(100),
    UNIQUE (city_name, country_name)
);
CREATE TABLE IF NOT EXISTS dim_seller_country (
    seller_country_id SERIAL PRIMARY KEY,
    country_name      VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_supplier_country (
    supplier_country_id SERIAL PRIMARY KEY,
    country_name        VARCHAR(100) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id  SERIAL PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    age          INTEGER,
    email        VARCHAR(200) UNIQUE,
    country      VARCHAR(100),
    postal_code  VARCHAR(20),
    pet_type     VARCHAR(50),
    pet_name     VARCHAR(100),
    pet_breed    VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id   SERIAL PRIMARY KEY,
    first_name  VARCHAR(100),
    last_name   VARCHAR(100),
    email       VARCHAR(200) UNIQUE,
    country     VARCHAR(100),
    postal_code VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id   SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200),
    contact_name  VARCHAR(200),
    email         VARCHAR(200) UNIQUE,
    phone         VARCHAR(50),
    address       VARCHAR(200),
    city          VARCHAR(100),
    country       VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS dim_store (
    store_id   SERIAL PRIMARY KEY,
    store_name VARCHAR(200),
    location   VARCHAR(200),
    city       VARCHAR(100),
    state      VARCHAR(100),
    country    VARCHAR(100),
    phone      VARCHAR(50),
    email      VARCHAR(200) UNIQUE
);
CREATE TABLE IF NOT EXISTS dim_product (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    category     VARCHAR(100),
    brand        VARCHAR(100),
    price        NUMERIC(10,2),
    weight       NUMERIC(10,2),
    color        VARCHAR(50),
    size         VARCHAR(50),
    material     VARCHAR(100),
    rating       NUMERIC(3,1),
    reviews      INTEGER,
    pet_category VARCHAR(100),
    UNIQUE (product_name, brand)
);
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id          SERIAL PRIMARY KEY,
    sale_date        DATE,
    customer_email   VARCHAR(200),
    seller_email     VARCHAR(200),
    product_name     VARCHAR(200),
    product_brand    VARCHAR(100),
    store_email      VARCHAR(200),
    supplier_email   VARCHAR(200),
    sale_quantity    INTEGER,
    sale_total_price NUMERIC(10,2)
);
""")
conn.commit()
cur.close()
conn.close()
print("DDL executed")

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
