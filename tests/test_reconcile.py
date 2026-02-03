import pandas as pd
from src.etl import reconcile_customers_orders

def test_orders_without_customers():
    customers_df = pd.DataFrame({
        "customer_id": [1, 2],
        "email": ["customer1@example.com", "customer2@example.com"],
        "signup_date": ["01/01/2023", "02/01/2023"]
    })

    orders_df = pd.DataFrame({
        "order_id": [10, 20, 30],
        "customer_id": [1, 2, 3],
        "amount": [100.0, 150.0, 200.0],
        "order_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    result = reconcile_customers_orders(customers_df, orders_df)

    assert len(result["orders_without_customers"]) == 1
    assert len(result["customers_without_orders"]) == 0




def test_reconcile_customers_orders():
    customers_df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "email": ["customer1@example.com", "customer2@example.com", "customer3@example.com"],
        "signup_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    orders_df = pd.DataFrame({
        "order_id": [10, 20, 30],
        "customer_id": [1, 2, 3],
        "amount": [100.0, 150.0, 200.0],
        "order_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    result = reconcile_customers_orders(customers_df, orders_df)

    assert len(result["orders_without_customers"]) == 0
    assert len(result["customers_without_orders"]) == 0
    assert len(result["summary"]) == 4



def test_customers_without_orders():
    customers_df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "email": ["customer1@example.com", "customer2@example.com", "customer3@example.com"],
        "signup_date": ["01/01/2023", "02/01/2023", "03/01/2023"]
    })

    orders_df = pd.DataFrame({
        "order_id": [10, 20],
        "customer_id": [1, 2],
        "amount": [100.0, 150.0],
        "order_date": ["01/01/2023", "02/01/2023"]
    })

    result = reconcile_customers_orders(customers_df, orders_df)

    assert len(result["orders_without_customers"]) == 0
    assert len(result["customers_without_orders"]) == 1

def test_combination():
    customers_df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4],
        "email": ["customer1@example.com", "customer2@example.com", "customer3@example.com", "customer4@example.com"],
        "signup_date": ["01/01/2023", "02/01/2023", "03/01/2023", "04/01/2023"]
    })

    orders_df = pd.DataFrame({
        "order_id": [10, 20, 30, 40],
        "customer_id": [1, 2, 2, 5],
        "amount": [100.0, 150.0, 200.0, 250.0],
        "order_date": ["01/01/2023", "02/01/2023", "03/01/2023", "04/01/2023"]
    })

    result = reconcile_customers_orders(customers_df, orders_df)

    assert len(result["orders_without_customers"]) == 1
    assert len(result["customers_without_orders"]) == 2
    assert result["summary"]["total_customers"] == 4

def test_empty_dataframes():
    customers_df = pd.DataFrame(columns=["customer_id", "email", "signup_date"])
    orders_df = pd.DataFrame(columns=["order_id", "customer_id", "amount", "order_date"])

    result = reconcile_customers_orders(customers_df, orders_df)

    assert len(result["orders_without_customers"]) == 0
    assert len(result["customers_without_orders"]) == 0
    assert result["summary"]["total_customers"] == 0
    assert result["summary"]["total_orders"] == 0
