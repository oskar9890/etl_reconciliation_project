import pandas as pd
from src.etl import clean_customers, clean_orders, reconcile_customers_orders

def test_full_reconciliation_workflow():
    customers_df = pd.DataFrame({
        "customer_id": [1,2,3],
        "email":["a","b","c"],
        "signup_date":["01/01/2023","02/01/2023","03/01/2023"]
    })
    orders_df = pd.DataFrame({
        "order_id":[1,2,3,4],
        "customer_id":[1,2,4,3],
        "amount":[10,20,30,40],
        "order_date":["01/01/2023","02/01/2023","03/01/2023","04/01/2023"]
    })

    # clean first
    customers_df = clean_customers(customers_df)
    orders_df = clean_orders(orders_df)

    result = reconcile_customers_orders(customers_df, orders_df)

    assert "orders_without_customers" in result
    assert "customers_without_orders" in result
    assert "summary" in result
    assert result["summary"]["total_customers"] == 3
    assert result["summary"]["total_orders"] == 4
