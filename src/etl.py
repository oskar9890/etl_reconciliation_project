import pandas as pd
import re

# ----------------------------
# Constants
# ----------------------------
EMAIL_REGEX = r"[^@]+@[^@]+\.[^@]+"

# ----------------------------
# Helper functions
# ----------------------------

def normalize_customer_id(series: pd.Series) -> pd.Series:
    """
    Normalise customer_id.
    Handles float-to-string issues.
    """
    return (
        series
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.lower()
    )

# ----------------------------
# CUSTOMER FUNCTIONS
# ----------------------------
def validate_customers(df: pd.DataFrame) -> None:
    """
    Validate cleaned customer data.
    """
    REQUIRED_COLUMNS = {"customer_id", "email", "signup_date"}
    missing = REQUIRED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(f"Missing required customer columns: {missing}")

    if df.empty:
        raise ValueError("Customer dataset is empty")

    if df["customer_id"].duplicated().any():
        raise ValueError("Duplicate customer IDs found")


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean customer data:
    - Drop rows missing customer_id
    - Normalize customer_id (lowercase, trimmed)
    - Flag invalid emails
    - Flag invalid signup dates
    - Convert signup_date using UK format (DD/MM/YYYY)
    """
    
    # Drop rows missing customer_id
    df = df.dropna(subset=["customer_id"]).copy()

    # Normalize customer IDs to avoid case-sensitive mismatches
    df["customer_id"] = (
        df["customer_id"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    # Fail if duplicates appear after normalization
    if df["customer_id"].duplicated().any():
        raise ValueError("Duplicate customer IDs found")

    # Flag invalid emails 
    df["email_valid"] = df["email"].astype(str).str.match(EMAIL_REGEX)

    # Convert signup_date and flag invalid values
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce", dayfirst=True)
    df["signup_date_valid"] = ~df["signup_date"].isna()

    return df


# ----------------------------
# ORDER FUNCTIONS
# ----------------------------
def validate_orders(df: pd.DataFrame) -> None:
    """
    Validate cleaned order data.
    """
    REQUIRED_COLUMNS = {"order_id", "customer_id", "amount", "order_date"}
    missing = REQUIRED_COLUMNS - set(df.columns)

    if missing:
        raise ValueError(f"Missing required order columns: {missing}")

    if df.empty:
        raise ValueError("Orders dataset is empty")

    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order IDs found")


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean orders data:
    - Drop rows missing order_id or customer_id
    - Convert amount to numeric and flag invalid values
    - Convert order_date and flag invalid values
    """

    # Drop rows missing required identifiers
    df = df.dropna(subset=["order_id", "customer_id"]).copy()

    # NORMALIZE customer_id 
    df["customer_id"] = (
        df["customer_id"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    if df["order_id"].duplicated().any():
        raise ValueError("Duplicate order IDs found")

    # Convert amount to numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["amount_valid"] = ~df["amount"].isna()

    # Convert order_date to datetime
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce", dayfirst=True)
    df["order_date_valid"] = ~df["order_date"].isna()

    return df


# ----------------------------
# RECONCILIATION
# ----------------------------
def reconcile_customers_orders(customers_df: pd.DataFrame, orders_df: pd.DataFrame):
    """
    Reconcile customers and orders.

    """

    customers_df = customers_df.copy()
    orders_df = orders_df.copy()
    customers_df["customer_id"] = normalize_customer_id(customers_df["customer_id"])
    orders_df["customer_id"] = normalize_customer_id(orders_df["customer_id"])


    orders_without_customers = orders_df[
        ~orders_df["customer_id"].isin(customers_df["customer_id"])
    ]

    customers_without_orders = customers_df[
        ~customers_df["customer_id"].isin(orders_df["customer_id"])
    ]

    summary = {
        "total_customers": len(customers_df),
        "total_orders": len(orders_df),
        "orders_without_customers": len(orders_without_customers),
        "customers_without_orders": len(customers_without_orders),
    }

    return {
        "orders_without_customers": orders_without_customers,
        "customers_without_orders": customers_without_orders,
        "summary": summary,
    }


def build_reconciled_dataset(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a combined orders + customers dataset.
    Orders are the base (LEFT JOIN).
    """
    customers_df = customers_df.copy()
    orders_df = orders_df.copy()
    customers_df["customer_id"] = normalize_customer_id(customers_df["customer_id"])
    orders_df["customer_id"] = normalize_customer_id(orders_df["customer_id"])


    combined = orders_df.merge(
        customers_df,
        on="customer_id",
        how="left",
        indicator=True
    )

    combined["customer_exists"] = combined["_merge"] == "both"
    combined.drop(columns="_merge", inplace=True)

    for col in ["email_valid", "signup_date_valid"]:
        if col in combined.columns:
            combined[col] = combined[col].fillna(False).astype(bool)

    return combined
