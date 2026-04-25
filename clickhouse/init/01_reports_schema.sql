CREATE DATABASE IF NOT EXISTS reports;

DROP TABLE IF EXISTS reports.report_top_products;
DROP TABLE IF EXISTS reports.report_revenue_by_category;
DROP TABLE IF EXISTS reports.report_product_ratings;
DROP TABLE IF EXISTS reports.report_top_customers;
DROP TABLE IF EXISTS reports.report_customers_by_country;
DROP TABLE IF EXISTS reports.report_customer_avg_check;
DROP TABLE IF EXISTS reports.report_monthly_trends;
DROP TABLE IF EXISTS reports.report_yearly_trends;
DROP TABLE IF EXISTS reports.report_top_stores;
DROP TABLE IF EXISTS reports.report_sales_by_location;
DROP TABLE IF EXISTS reports.report_top_suppliers;
DROP TABLE IF EXISTS reports.report_supplier_by_country;
DROP TABLE IF EXISTS reports.report_product_quality;

DROP TABLE IF EXISTS reports.report_products;
CREATE TABLE reports.report_products (
    product_name String,
    brand String,
    category String,
    total_units_sold Int64,
    total_revenue Float64,
    sales_count Int64,
    avg_rating Float64,
    reviews_count Int64,
    category_total_revenue Float64,
    category_total_units_sold Int64,
    category_sales_count Int64,
    product_sales_rank Int64
) ENGINE = MergeTree() ORDER BY product_sales_rank;

DROP TABLE IF EXISTS reports.report_customers;
CREATE TABLE reports.report_customers (
    country String,
    email String,
    first_name String,
    last_name String,
    total_spent Float64,
    orders_count Int64,
    avg_check Float64,
    country_unique_customers Int64,
    country_total_revenue Float64,
    country_avg_order_value Float64,
    customer_spending_rank Int64
) ENGINE = MergeTree() ORDER BY customer_spending_rank;

DROP TABLE IF EXISTS reports.report_time;
CREATE TABLE reports.report_time (
    year Int32,
    month Int32,
    period String,
    total_revenue Float64,
    sales_count Int64,
    avg_order_value Float64,
    total_units_sold Int64,
    period_type String,
    year_total_revenue Float64,
    year_sales_count Int64,
    year_avg_order_value Float64,
    previous_period_revenue Nullable(Float64),
    revenue_diff_vs_previous_period Nullable(Float64),
    revenue_pct_diff_vs_previous_period Nullable(Float64)
) ENGINE = MergeTree() ORDER BY (year, month);

DROP TABLE IF EXISTS reports.report_stores;
CREATE TABLE reports.report_stores (
    city String,
    country String,
    email String,
    store_name String,
    total_revenue Float64,
    sales_count Int64,
    avg_check Float64,
    location_total_revenue Float64,
    location_sales_count Int64,
    location_avg_check Float64,
    store_revenue_rank Int64
) ENGINE = MergeTree() ORDER BY store_revenue_rank;

DROP TABLE IF EXISTS reports.report_suppliers;
CREATE TABLE reports.report_suppliers (
    country String,
    email String,
    supplier_name String,
    total_revenue Float64,
    sales_count Int64,
    avg_product_price Float64,
    country_total_revenue Float64,
    country_sales_count Int64,
    supplier_revenue_rank Int64
) ENGINE = MergeTree() ORDER BY supplier_revenue_rank;

DROP TABLE IF EXISTS reports.report_quality;
CREATE TABLE reports.report_quality (
    product_name String,
    brand String,
    category String,
    total_units_sold Int64,
    total_revenue Float64,
    sales_count Int64,
    avg_rating Float64,
    reviews_count Int64,
    rating_sales_correlation Nullable(Float64),
    rating_rank_desc Int64,
    rating_rank_asc Int64,
    reviews_rank Int64
) ENGINE = MergeTree() ORDER BY rating_rank_desc;
