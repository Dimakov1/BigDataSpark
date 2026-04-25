# Лабораторная работа №2

## ETL-пайплайн на Apache Spark с выгрузкой отчётов в ClickHouse

---

## 📌 Описание

В данной лабораторной работе реализован ETL-пайплайн на **Apache Spark**, который:

1. Загружает исходные данные из CSV-файлов в PostgreSQL (таблица `mock_data`) — автоматически при старте контейнера
2. Трансформирует данные из `mock_data` в аналитическую модель **звезда** в PostgreSQL
3. На основе модели звезда создаёт **6 аналитических витрин** в **ClickHouse**

---

## 🧱 Модель данных (звезда в PostgreSQL)

### Таблица фактов
* `fact_sales` — продажи (дата, email покупателя, email продавца, название и бренд товара, email магазина, email поставщика, кол-во, сумма)

### Таблицы измерений
* `dim_customer` — покупатели (имя, возраст, email, страна, питомец)
* `dim_seller` — продавцы (имя, email, страна)
* `dim_product` — товары (название, категория, бренд, цена, рейтинг, отзывы)
* `dim_store` — магазины (название, город, страна, email)
* `dim_supplier` — поставщики (название, email, город, страна)

Связь между `fact_sales` и измерениями осуществляется через **email** (суррогатный ключ).

---

## 📊 Аналитические витрины в ClickHouse

Все 6 витрин объединены в **6 денормализованных таблиц** — каждая содержит как детальные, так и агрегированные данные по своему срезу.

| # | Витрина | Таблица в ClickHouse | Ключевые метрики |
|---|---------|----------------------|-----------------|
| 1 | Продажи по продуктам | `report_products` | выручка, кол-во продаж, рейтинг, ранг по продажам, агрегаты по категории |
| 2 | Продажи по клиентам | `report_customers` | сумма покупок, кол-во заказов, средний чек, ранг, агрегаты по стране |
| 3 | Продажи по времени | `report_time` | месячная/годовая выручка, динамика, % изменения к предыдущему периоду |
| 4 | Продажи по магазинам | `report_stores` | выручка, средний чек, ранг, агрегаты по городу/стране |
| 5 | Продажи по поставщикам | `report_suppliers` | выручка, средняя цена товара, ранг, агрегаты по стране |
| 6 | Качество продукции | `report_quality` | рейтинг, отзывы, корреляция рейтинга с продажами, ранги |

---

## 📁 Структура проекта

```text
BigDataSpark/
├── docker-compose.yml
├── README_fixed.md
├── sql/
│   └── 01_init_raw.sql              — создание таблицы mock_data и загрузка CSV
├── clickhouse/
│   └── init/
│       └── 01_reports_schema.sql    — DDL всех 6 таблиц в ClickHouse
├── spark/
│   ├── 01_etl_to_star.py            — ETL: mock_data → схема звезда (PostgreSQL)
│   ├── 02_reports_clickhouse.py     — отчёты: PostgreSQL → ClickHouse
│   ├── requirements.txt
│   └── jars/
│       ├── postgresql-42.7.3.jar
│       └── clickhouse-jdbc-0.9.8-all.jar
└── исходные данные/
    ├── MOCK_DATA.csv
    ├── MOCK_DATA (1).csv
    └── ... (всего 10 файлов по 1000 строк)
```

---

## ⚙️ Как запустить проект

### Шаг 1 — Запустить все сервисы

```bash
docker-compose up -d
```

Запустятся: **PostgreSQL**, **ClickHouse**, **Spark Master**.

### Шаг 2 — Запустить ETL: mock_data → схема звезда (PostgreSQL)

```
docker exec -it spark_master /opt/spark/bin/spark-submit `
  --jars /opt/spark-jobs/jars/postgresql-42.7.3.jar `
  /opt/spark-jobs/01_etl_to_star.py
```

### Шаг 3 — Запустить создание отчётов в ClickHouse

```
docker exec -it spark_master /opt/spark/bin/spark-submit `
  --jars /opt/spark-jobs/jars/postgresql-42.7.3.jar,/opt/spark-jobs/jars/clickhouse-jdbc-0.9.8-all.jar `
  /opt/spark-jobs/02_reports_clickhouse.py
```

Скрипт заполнит все 6 таблиц в базе `reports` ClickHouse.

---

## ✅ Проверка результатов

### PostgreSQL (через DBeaver)

Подключение: `localhost:5432`, база `petshop`, user/pass: `postgres`

```sql
-- Проверка исходных данных
SELECT COUNT(*) FROM mock_data;  -- должно быть 10000

-- Проверка схемы звезда
SELECT 'fact_sales',   COUNT(*) FROM fact_sales
UNION ALL SELECT 'dim_customer',  COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller',    COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product',   COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store',     COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier',  COUNT(*) FROM dim_supplier;
```

### ClickHouse (через DBeaver или HTTP)

Подключение: `localhost:8123`, база `reports`, user: `default`, пароль пустой

```sql
-- Топ продуктов по продажам
SELECT product_name, brand, total_units_sold, total_revenue
FROM reports.report_products
ORDER BY product_sales_rank
LIMIT 10;

-- Топ клиентов по сумме покупок
SELECT first_name, last_name, country, total_spent, orders_count, avg_check
FROM reports.report_customers
ORDER BY customer_spending_rank
LIMIT 10;

-- Месячные тренды продаж
SELECT period, total_revenue, sales_count, revenue_pct_diff_vs_previous_period
FROM reports.report_time
ORDER BY year, month;

-- Топ магазинов по выручке
SELECT store_name, city, country, total_revenue, avg_check
FROM reports.report_stores
ORDER BY store_revenue_rank
LIMIT 5;

-- Топ поставщиков по выручке
SELECT supplier_name, country, total_revenue, avg_product_price
FROM reports.report_suppliers
ORDER BY supplier_revenue_rank
LIMIT 5;

-- Качество продукции (корреляция рейтинга и продаж)
SELECT product_name, brand, avg_rating, reviews_count, total_units_sold, rating_sales_correlation
FROM reports.report_quality
ORDER BY rating_rank_desc
LIMIT 10;
```

Или через HTTP API:

```bash
curl "http://localhost:8123/?query=SELECT+product_name,total_units_sold+FROM+reports.report_products+ORDER+BY+product_sales_rank+LIMIT+5"
```

---

## 🔧 Технические детали реализации

### Соединение Spark ↔ PostgreSQL
Используется JDBC-драйвер `postgresql-42.7.3.jar`. Spark читает таблицы через `spark.read.jdbc()` и пишет через `df.write.jdbc()`.

### Соединение Spark ↔ ClickHouse
Используется `clickhouse-jdbc-0.9.8-all.jar` с драйвером `com.clickhouse.jdbc.ClickHouseDriver`. Запись через `df.write.format("jdbc")` в режиме `append` (таблицы создаются заранее DDL-скриптом).

### Дедупликация в ETL
При построении измерений применяется `dropDuplicates()` по email — это гарантирует уникальность записей в `dim_*` таблицах.

### Оконные функции в отчётах
Ранжирование (`ROW_NUMBER`) и вычисление динамики (`LAG`) реализованы через `pyspark.sql.Window` без выгрузки данных в память.

---

## 🎓 Вывод

В ходе работы были освоены:

* построение ETL-пайплайна на **Apache Spark (PySpark)**
* трансформация данных из плоской таблицы в модель **звезда** в PostgreSQL
* запись данных из Spark в **PostgreSQL** через JDBC
* создание денормализованных аналитических витрин в **ClickHouse** с использованием оконных функций
* автоматическая инициализация схем БД через `docker-entrypoint-initdb.d`
* оркестрация сервисов через **Docker Compose** с healthcheck-зависимостями

---

Студент: Ковриженков Дмитрий Олегович  
Группа: М80-303Б-23
