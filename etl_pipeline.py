"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
import logging
from datetime import datetime 

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )

def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_metadata_table(engine):
    """Create etl_metadata table if it does not exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS etl_metadata (
        run_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP,
        rows_processed INTEGER,
        status VARCHAR(50) NOT NULL
    );
    """

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))


def get_last_successful_run(engine):
    """Return the end_time of the last successful ETL run."""
    query = """
    SELECT MAX(end_time) AS last_successful_run
    FROM etl_metadata
    WHERE status = 'success'
    """

    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()

    if result and result[0]:
        return result[0]
    return None

def log_etl_run(engine, start_time, end_time, rows_processed, status):
    """Log ETL run details into etl_metadata."""
    insert_sql = """
    INSERT INTO etl_metadata (start_time, end_time, rows_processed, status)
    VALUES (:start_time, :end_time, :rows_processed, :status)
    """

    with engine.begin() as conn:
        conn.execute(
            text(insert_sql),
            {
                "start_time": start_time,
                "end_time": end_time,
                "rows_processed": rows_processed,
                "status": status,
            }
        )

def extract(engine, last_run_time=None):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database
        last_run_time: timestamp of the last successful ETL run (optional)

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)

    if last_run_time is None:
        orders = pd.read_sql("SELECT * FROM orders", engine)
    else:
        orders_query = """
        SELECT * FROM orders
        WHERE order_date > %(last_run_time)s
        """
        orders = pd.read_sql(orders_query, engine, params={"last_run_time": last_run_time})

    if orders.empty:
        order_items = pd.DataFrame(columns=["item_id", "order_id", "product_id", "quantity"])
    else:
        order_ids = tuple(orders["order_id"].tolist())

        if len(order_ids) == 1:
            order_items_query = f"SELECT * FROM order_items WHERE order_id = {order_ids[0]}"
        else:
            order_items_query = f"SELECT * FROM order_items WHERE order_id IN {order_ids}"

        order_items = pd.read_sql(order_items_query, engine)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }




def transform(data_dict):
    """Transform raw data into customer-level analytics summary.

    Steps:
    1. Join orders with order_items and products
    2. Compute line_total (quantity * unit_price)
    3. Filter out cancelled orders (status = 'cancelled')
    4. Filter out suspicious quantities (quantity > 100)
    5. Aggregate to customer level: total_orders, total_revenue,
       avg_order_value, top_category

    Args:
        data_dict: dict of DataFrames from extract()

    Returns:
        DataFrame: customer-level summary with columns:
            customer_id, customer_name, city, total_orders,
            total_revenue, avg_order_value, top_category
    """
    customers = data_dict["customers"].copy()
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="inner")

    merged = merged[merged["status"] != "cancelled"]
    merged = merged[merged["quantity"] <= 100]

    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    if "customer_name" in customers.columns:
     customer_base = customers[["customer_id", "customer_name", "city"]]
    elif "name" in customers.columns:
     customer_base = customers[["customer_id", "name", "city"]].rename(
        columns={"name": "customer_name"}
    )
    else:
     raise KeyError("customers DataFrame must contain either 'customer_name' or 'name'")

    customer_summary = (
        merged.groupby("customer_id", as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_revenue=("line_total", "sum"),
        )
    )

    customer_summary["avg_order_value"] = (
        customer_summary["total_revenue"] / customer_summary["total_orders"]
    )

    category_revenue = (
        merged.groupby(["customer_id", "category"], as_index=False)
        .agg(category_revenue=("line_total", "sum"))
    )

    category_revenue = category_revenue.sort_values(
        by=["customer_id", "category_revenue", "category"],
        ascending=[True, False, True]
    )

    top_category = (
        category_revenue.drop_duplicates(subset=["customer_id"])[["customer_id", "category"]]
        .rename(columns={"category": "top_category"})
    )

    customer_summary = customer_summary.merge(customer_base, on="customer_id", how="left")
    customer_summary = customer_summary.merge(top_category, on="customer_id", how="left")

    customer_summary = customer_summary[
        [
            "customer_id",
            "customer_name",
            "city",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "top_category",
        ]
    ]
    mean_revenue = customer_summary["total_revenue"].mean()
    std_revenue = customer_summary["total_revenue"].std()

    threshold = mean_revenue + (3 * std_revenue)

    customer_summary["is_outlier"] = customer_summary["total_revenue"] > threshold
    return customer_summary

def transform_with_config(data_dict, config):
    customers = data_dict["customers"].copy()
    products = data_dict["products"].copy()
    orders = data_dict["orders"].copy()
    order_items = data_dict["order_items"].copy()

    merged = orders.merge(order_items, on="order_id", how="inner")
    merged = merged.merge(products, on="product_id", how="inner")

    merged = merged[merged["status"] != "cancelled"]
    merged = merged[merged["quantity"] <= 100]

    merged["line_total"] = merged["quantity"] * merged["unit_price"]

    if "customer_name" in customers.columns:
        customer_base = customers[["customer_id", "customer_name", "city"]]
    elif "name" in customers.columns:
        customer_base = customers[["customer_id", "name", "city"]].rename(
            columns={"name": "customer_name"}
        )
    else:
        customer_base = pd.DataFrame()

    if config["pipeline_name"] == "customer_pipeline":
        agg_dict = {
            metric_name: (metric_def["column"], metric_def["agg"])
            for metric_name, metric_def in config["metrics"].items()
        }

        summary = (
            merged.groupby(config["group_by"], as_index=False)
            .agg(**agg_dict)
        )

        summary["avg_order_value"] = (
            summary["total_revenue"] / summary["total_orders"]
        )

        category_revenue = (
            merged.groupby(["customer_id", "category"], as_index=False)
            .agg(category_revenue=("line_total", "sum"))
        )

        category_revenue = category_revenue.sort_values(
            by=["customer_id", "category_revenue", "category"],
            ascending=[True, False, True]
        )

        top_category = (
            category_revenue.drop_duplicates(subset=["customer_id"])[["customer_id", "category"]]
            .rename(columns={"category": "top_category"})
        )

        summary = summary.merge(customer_base, on="customer_id", how="left")
        summary = summary.merge(top_category, on="customer_id", how="left")

        summary = summary[
            [
                "customer_id",
                "customer_name",
                "city",
                "total_orders",
                "total_revenue",
                "avg_order_value",
                "top_category",
            ]
        ]

    elif config["pipeline_name"] == "product_pipeline":
        merged = merged.rename(columns={"name": "product_name"})

        agg_dict = {
            metric_name: (metric_def["column"], metric_def["agg"])
            for metric_name, metric_def in config["metrics"].items()
        }

        summary = (
            merged.groupby(config["group_by"], as_index=False)
            .agg(**agg_dict)
        )

    else:
        raise ValueError(f"Unsupported pipeline_name: {config['pipeline_name']}")

    outlier_col = config.get("outlier_column")
    if outlier_col and outlier_col in summary.columns:
        mean_value = summary[outlier_col].mean()
        std_value = summary[outlier_col].std()
        threshold = mean_value + (3 * std_value)
        summary["is_outlier"] = summary[outlier_col] > threshold

    return summary


def validate(df):
    """Run data quality checks on the transformed DataFrame.

    Checks:
    - No nulls in customer_id or customer_name
    - total_revenue > 0 for all customers
    - No duplicate customer_ids
    - total_orders > 0 for all customers

    Args:
        df: transformed customer summary DataFrame

    Returns:
        dict: {check_name: bool} for each check

    Raises:
        ValueError: if any critical check fails
    """
    checks = {
        "no_null_customer_id": df["customer_id"].notna().all(),
        "no_null_customer_name": df["customer_name"].notna().all(),
        "total_revenue_positive": (df["total_revenue"] > 0).all(),
        "no_duplicate_customer_id": ~df["customer_id"].duplicated().any(),
        "total_orders_positive": (df["total_orders"] > 0).all(),
    }

    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check_name}: {status}")

    if not all(checks.values()):
        raise ValueError("Data validation failed")

    return checks

def generate_quality_report(df, validation_results, report_path):
    """Generate a JSON quality report for the ETL run."""
    output_dir = os.path.dirname(report_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    outliers_df = df[df["is_outlier"]][["customer_id", "total_revenue"]]

    clean_validation_results = {
    key: bool(value) for key, value in validation_results.items()
}

    report = {
    "etl_run_timestamp": datetime.now().isoformat(),
    "total_records_checked": int(len(df)),
    "checks": clean_validation_results,
    "checks_passed": int(sum(clean_validation_results.values())),
    "checks_failed": int(len(clean_validation_results) - sum(clean_validation_results.values())),
    "flagged_outliers": outliers_df.to_dict(orient="records"),
}

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Quality report saved to {report_path}")


def load(df, engine, csv_path, table_name="customer_summary"):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
        table_name: destination table name
    """
    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    logging.info(f"Loaded {len(df)} rows into {table_name}")




def main(config_path="config/customer_pipeline.json"):
    """Orchestrate the ETL pipeline."""
    setup_logging()
    config = load_config(config_path)

    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    )

    engine = create_engine(database_url)
    start_time = datetime.now()

    try:
        ensure_metadata_table(engine)

        last_run_time = get_last_successful_run(engine)

        if last_run_time:
            logging.info(f"Starting incremental extract since {last_run_time}...")
        else:
            logging.info("Starting full extract...")

        data_dict = extract(engine, last_run_time=last_run_time)

        for table_name, table_df in data_dict.items():
            logging.info(f"{table_name}: {len(table_df)} rows")

        if data_dict["orders"].empty:
            logging.info("No new orders found. ETL skipped.")
            end_time = datetime.now()
            log_etl_run(engine, start_time, end_time, 0, "success")
            return

        logging.info("Starting transform...")
        final_df = transform_with_config(data_dict, config)
        logging.info(f"{config['pipeline_name']}: {len(final_df)} rows")

        if config["pipeline_name"] == "customer_pipeline":
            logging.info("Starting validate...")
            validation_results = validate(final_df)

            logging.info("Generating quality report...")
            generate_quality_report(
                final_df,
                validation_results,
                "output/quality_report.json"
            )

        logging.info("Starting load...")
        load(
            final_df,
            engine,
            config["output_csv"],
            table_name=config["output_table"]
        )

        end_time = datetime.now()
        log_etl_run(engine, start_time, end_time, len(final_df), "success")

        logging.info("ETL pipeline completed successfully.")

    except Exception:
        end_time = datetime.now()
        log_etl_run(engine, start_time, end_time, 0, "failed")
        raise


if __name__ == "__main__":
    main("config/customer_pipeline.json")