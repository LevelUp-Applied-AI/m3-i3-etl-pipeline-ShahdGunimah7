"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    """Extract all source tables from PostgreSQL into DataFrames.

    Args:
        engine: SQLAlchemy engine connected to the amman_market database

    Returns:
        dict: {"customers": df, "products": df, "orders": df, "order_items": df}
    """
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

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

    return customer_summary


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


def load(df, engine, csv_path):
    """Load customer summary to PostgreSQL table and CSV file.

    Args:
        df: validated customer summary DataFrame
        engine: SQLAlchemy engine
        csv_path: path for CSV output
    """
    output_dir = os.path.dirname(csv_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_sql("customer_summary", engine, if_exists="replace", index=False)
    df.to_csv(csv_path, index=False)

    print(f"Loaded {len(df)} rows")


def main():
    """Orchestrate the ETL pipeline: extract -> transform -> validate -> load."""
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    )

    engine = create_engine(database_url)

    print("Starting extract...")
    data_dict = extract(engine)
    for table_name, table_df in data_dict.items():
        print(f"{table_name}: {len(table_df)} rows")

    print("Starting transform...")
    customer_df = transform(data_dict)
    print(f"customer_summary: {len(customer_df)} rows")

    print("Starting validate...")
    validate(customer_df)

    print("Starting load...")
    load(customer_df, engine, "output/customer_analytics.csv")

    print("ETL pipeline completed successfully.")


if __name__ == "__main__":
    main()