[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Nvxy3054)
# ETL Pipeline — Amman Digital Market

## Overview

This ETL pipeline extracts data from a PostgreSQL database, transforms it into customer-level analytics, validates data quality, and loads the results into a new table and CSV file.

The pipeline processes order, product, and customer data to generate insights such as total revenue, average order value, and top product category per customer.

## Setup

1. Start PostgreSQL container:
   ```bash
   docker run -d --name postgres-m3-int \
     -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=amman_market \
     -p 5432:5432 -v pgdata_m3_int:/var/lib/postgresql/data \
     postgres:15-alpine
   ```
2. Load schema and data:
   ```bash
   psql -h localhost -U postgres -d amman_market -f schema.sql
   psql -h localhost -U postgres -d amman_market -f seed_data.sql
   ```
3. Install dependencies: `pip install -r requirements.txt`

## How to Run

```bash
python etl_pipeline.py
```

## Output

The output file `customer_analytics.csv` contains a customer-level summary with the following columns:

- customer_id
- customer_name
- city
- total_orders (number of distinct orders)
- total_revenue (sum of all order values)
- avg_order_value (average revenue per order)
- top_category (category with the highest revenue for the customer)

## Quality Checks

The pipeline includes several data quality checks to ensure reliability:

- No null values in `customer_id` or `customer_name` (ensures valid customer records)
- `total_revenue > 0` (ensures meaningful revenue data)
- No duplicate `customer_id` values (ensures uniqueness per customer)
- `total_orders > 0` (ensures customers have valid transactions)

If any of these checks fail, a ValueError is raised to prevent loading invalid data.
---

## License

This repository is provided for educational use only. See [LICENSE](LICENSE) for terms.

You may clone and modify this repository for personal learning and practice, and reference code you wrote here in your professional portfolio. Redistribution outside this course is not permitted.
