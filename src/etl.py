import pandas as pd
import json
import re
import os
os.makedirs("output", exist_ok=True)


# ----------------------------
# Constants
# ----------------------------
EMAIL_REGEX = r"[^@]+@[^@]+\.[^@]+"


# ----------------------------
# CUSTOMER FUNCTIONS
# ----------------------------
def load_customers_csv() -> pd.DataFrame:
    """
    Load customers CSV, clean it, validate it, and return a DataFrame.
    """
    path = "data/customers.csv"
    df = pd.read_csv(path)

    # Ensure required columns exist
    REQUIRED_COLUMNS = {"customer_id", "email"}
    missing_required = REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    # Transform / clean data
    df = clean_customers(df)

    # Validate cleaned data
    validate_customers(df)

    return df


def validate_customers(df: pd.DataFrame) -> None:
    """
    Validate the cleaned customer data.
    Only checks for emptiness and duplicates (since cleaning already handles issues).
    """
    if df.empty:
        raise ValueError("Customer dataset is empty")

    if df["customer_id"].duplicated().any():
        print("WARNING: Duplicate customer IDs found")


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean customer data by:
    - dropping missing customer_id
    - removing duplicates
    - adding flags for invalid email and invalid signup date
    """
    original_count = len(df)

    # Drop rows missing customer_id (cannot reconcile without it)
    df = df.dropna(subset=["customer_id"])

    # Remove duplicate customer IDs
    df = df.drop_duplicates(subset=["customer_id"])

    # Flag invalid emails (but keep the row)
    df["email_valid"] = df["email"].str.match(EMAIL_REGEX)

    # Convert signup_date to datetime and flag invalid values
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df["signup_date_valid"] = ~df["signup_date"].isna()

    removed = original_count - len(df)
    print(f"Removed {removed} invalid customer records")

    return df


# ----------------------------
# ORDER FUNCTIONS
# ----------------------------
def load_orders_csv() -> pd.DataFrame:
    """
    Load orders CSV, clean it, validate it, and return a DataFrame.
    """
    path = "data/orders.csv"
    df = pd.read_csv(path)

    # Ensure required columns exist
    REQUIRED_COLUMNS = {"order_id", "customer_id", "amount"}
    missing_required = REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    # Transform / clean data
    df = clean_orders(df)

    # Validate cleaned data
    validate_orders(df)

    return df


def validate_orders(df: pd.DataFrame) -> None:
    """
    Validate the cleaned orders data.
    Only checks for emptiness and duplicates (since cleaning already handles issues).
    """
    if df.empty:
        raise ValueError("Orders dataset is empty")

    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order IDs found")


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean orders data by:
    - dropping rows missing order_id or customer_id
    - converting amount to numeric (flag invalid values)
    - flag invalid order dates
    """
    original_count = len(df)

    # Drop rows missing required IDs
    df = df.dropna(subset=["order_id", "customer_id"])

    # Convert amount to numeric and flag invalid values
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["amount_valid"] = ~df["amount"].isna()

    # Convert order_date to datetime and flag invalid values
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_date_valid"] = ~df["order_date"].isna()

    removed = original_count - len(df)
    print(f"Removed {removed} invalid order records")

    return df


# ----------------------------
# RECONCILIATION FUNCTION
# ----------------------------
def reconcile_customers_orders(customers_df, orders_df):
    """
    Reconcile customers and orders.

    Returns:
        dict with:
            - orders_without_customers (DataFrame)
            - customers_without_orders (DataFrame)
            - summary (dict)
    """
    # Orders that reference customer IDs that do not exist in customers_df
    orders_without_customers = orders_df[
        ~orders_df["customer_id"].isin(customers_df["customer_id"])
    ]

    # Customers that have no matching orders
    customers_without_orders = customers_df[
        ~customers_df["customer_id"].isin(orders_df["customer_id"])
    ]

    # Build summary statistics
    summary = {
        "total_customers": len(customers_df),
        "total_orders": len(orders_df),
        "orders_without_customers": len(orders_without_customers),
        "customers_without_orders": len(customers_without_orders),
    }

    return {
        "orders_without_customers": orders_without_customers,
        "customers_without_orders": customers_without_orders,
        "summary": summary
    }


# ----------------------------
# MAIN EXECUTION
# ----------------------------
if __name__ == "__main__":
    customers_df = load_customers_csv()
    orders_df = load_orders_csv()

    report = reconcile_customers_orders(customers_df, orders_df)

    print(report["summary"])
    print(report["orders_without_customers"].head())
    print(report["customers_without_orders"].head())

    customers_df.to_csv("output/clean_customers.csv", index=False)
    orders_df.to_csv("output/clean_orders.csv", index=False)

    report["orders_without_customers"].to_csv("output/orders_without_customers.csv", index=False)
    report["customers_without_orders"].to_csv("output/customers_without_orders.csv", index=False)
    
    with open("output/summary.json", "w") as f:
        json.dump(report["summary"], f, indent=2)