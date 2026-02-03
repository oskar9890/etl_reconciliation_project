import pandas as pd
from src.etl import validate_orders, clean_orders
import pytest

def test_empty_orders_dataframe():
    input_df = pd.DataFrame(columns=["order_id", "customer_id", "amount", "order_date"])

    with pytest.raises(ValueError, match="empty"):
        validate_orders(input_df)

def test_duplicate_order_ids():
    input_df = pd.DataFrame({
        "order_id": [1, 2, 2],
        "customer_id": [10, 20, 30],
        "amount": [100.0, 150.0, 200.0],
        "order_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    with pytest.raises(ValueError, match="Duplicate"):
        validate_orders(input_df)

def test_amount_conversion():
    input_df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "customer_id": [10, 20, 30],
        "amount": ["100.0", "invalid", "200.0"],
        "order_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    result_df = clean_orders(input_df)

    assert result_df["amount_valid"].tolist() == [True, False, True]
    assert pd.isna(result_df.loc[1, "amount"])

def test_invalid_order_date():
    input_df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "customer_id": [10, 20, 30],
        "amount": [100.0, 150.0, 200.0],
        "order_date": ["01/01/2023", "invalid-date", "03/01/2023"]
    })

    result_df = clean_orders(input_df)

    assert result_df["order_date_valid"].tolist() == [True, False, True]
    assert pd.isna(result_df.loc[1, "order_date"])
