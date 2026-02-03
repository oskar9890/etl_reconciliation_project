import pandas as pd
from src.etl import clean_customers
import pytest

def test_drop_rows_without_customer_id():
    input_df = pd.DataFrame({
        "customer_id": [1, None, 3],
        "email": ["a@test.com", "b@test.com", "c@test.com"],
        "signup_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    result_df = clean_customers(input_df)

    assert len(result_df) == 2
    assert result_df["customer_id"].isna().sum() == 0


def test_invalid_email_format():
    input_df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "email": ["test@test.com", "invalid-email", "another@test.com"],
        "signup_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    result_df = clean_customers(input_df)

    assert len(result_df) == 3
    assert result_df["email_valid"].tolist() == [True, False, True]


def test_invalid_signup_date():
    input_df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "email": ["test@gmail.com", "test2@gmail.com", "test3@gmail.com"],
        "signup_date": ["01/01/2023", "invalid-date", "03/01/2023"]
    })

    result_df = clean_customers(input_df)

    assert len(result_df) == 3
    assert result_df["signup_date_valid"].tolist() == [True, False, True]
    assert pd.isna(result_df.iloc[1]["signup_date"])


def test_remove_duplicate_customer_ids():
    input_df = pd.DataFrame({
        "customer_id": ["abc", "ABC", "def"],
        "email": ["test@gmail.com", "test2@gmail.com", "test3@gmail.com"],
        "signup_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    with pytest.raises(ValueError, match="Duplicate"):
        clean_customers(input_df)
