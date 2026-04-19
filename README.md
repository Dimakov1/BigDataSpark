# Лабораторная работа №2

## ETL-пайплайн на Apache Spark с выгрузкой отчётов в ClickHouse

---

## 📌 Описание

В данной лабораторной работе реализован ETL-пайплайн на **Apache Spark**, который:

1. Загружает исходные данные из CSV-файлов в PostgreSQL (таблица `mock_data`)
2. Трансформирует данные из `mock_data` в аналитическую модель **звезда** в PostgreSQL
3. На основе модели звезда создаёт **6 аналитических витрин** в **ClickHouse**

Работа является продолжением лабораторной работы №1 — используется та же исходная структура данных и та же схема нормализации.


## 🧱 Модель данных (звезда в PostgreSQL)

### Таблица фактов
* `fact_sales` — продажи (дата, покупатель, продавец, товар, магазин, поставщик, кол-во, сумма)

### Таблицы измерений
* `dim_customer` — покупатели
* `dim_seller` — продавцы
* `dim_product` — товары
* `dim_store` — магазины
* `dim_supplier` — поставщики

---

## 📊 Аналитические витрины в ClickHouse

| # | Витрина | Таблицы в ClickHouse |
|---|---------|----------------------|
| 1 | Продажи по продуктам | `report_top_products`, `report_revenue_by_category`, `report_product_ratings` |
| 2 | Продажи по клиентам | `report_top_customers`, `report_customers_by_country`, `report_customer_avg_check` |
| 3 | Продажи по времени | `report_monthly_trends`, `report_yearly_trends` |
| 4 | Продажи по магазинам | `report_top_stores`, `report_sales_by_location` |
| 5 | Продажи по поставщикам | `report_top_suppliers`, `report_supplier_by_country` |
| 6 | Качество продукции | `report_product_quality` |

---

## 📁 Структура проекта

```text
BigDataSpark-main/
├── docker-compose.yml
├── README.md
├── sql/
│   └── 01_init_raw.sql           — создание и загрузка mock_data
├── spark/
│   ├── 01_etl_to_star.py         — ETL: mock_data → схема звезда (PostgreSQL)
│   ├── 02_reports_clickhouse.py  — отчёты: PostgreSQL → ClickHouse
│   └── requirements.txt
└── исходные данные/
    ├── MOCK_DATA.csv
    ├── MOCK_DATA (1).csv
    └── ... (всего 10 файлов)
```

---

## ⚙️ Как запустить проект

### Шаг 1 — Скачать JDBC-драйвер

```bash
cd BigDataSpark-main/spark
bash download_jars.sh
```

### Шаг 2 — Запустить все сервисы

```bash
cd BigDataSpark-main
docker-compose up -d
```

Запустятся: **PostgreSQL**, **ClickHouse**, **Spark Master**, **Spark Worker**.

Дождитесь готовности (~60 сек):

```bash
docker logs spark_postgres   # ждём: database system is ready
docker logs spark_clickhouse # ждём: Ready for connections
```

### Шаг 3 — Установить зависимости Python в контейнере Spark

```bash
docker exec spark_master pip install psycopg2-binary clickhouse-connect pandas
```

### Шаг 4 — Запустить ETL: CSV → PostgreSQL (схема звезда)

```bash
docker exec spark_master spark-submit \
  --master spark://spark_master:7077 \
  --jars /opt/spark-jobs/jars/postgresql-42.7.3.jar \
  /opt/spark-jobs/01_etl_to_star.py
```

### Шаг 5 — Запустить создание отчётов в ClickHouse

```bash
docker exec spark_master spark-submit \
  --master spark://spark_master:7077 \
  --jars /opt/spark-jobs/jars/postgresql-42.7.3.jar \
  /opt/spark-jobs/02_reports_clickhouse.py
```

---

## ✅ Проверка результатов

### PostgreSQL (через DBeaver)

Подключение: `localhost:5432`, база `petshop`, user/pass: `postgres`

```sql
SELECT 'fact_sales',  COUNT(*) FROM fact_sales
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store',    COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_seller',   COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier;
```

### ClickHouse (через DBeaver или HTTP)

Подключение: `localhost:8123`, база `reports`, user: `default`

```sql
SELECT * FROM report_top_products ORDER BY total_units_sold DESC;
SELECT * FROM report_revenue_by_category;
SELECT * FROM report_top_customers;
SELECT * FROM report_monthly_trends ORDER BY year, month;
SELECT * FROM report_top_stores;
SELECT * FROM report_product_quality ORDER BY rating DESC;
```

Или через HTTP:

```bash
curl "http://localhost:8123/?query=SELECT+*+FROM+reports.report_top_products+LIMIT+5"
```

---

## 🎓 Вывод

В ходе работы были освоены:

* построение ETL-пайплайна на **Apache Spark (PySpark)**
* трансформация данных из плоской таблицы в модель **звезда**
* запись данных из Spark в **PostgreSQL** через JDBC
* создание аналитических витрин в **ClickHouse**
* оркестрация сервисов через **Docker Compose**

---

Студент: Ковриженков Дмитрий Олегович
Группа: М80-303Б-23
