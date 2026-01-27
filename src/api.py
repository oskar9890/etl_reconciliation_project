from fastapi import FastAPI, UploadFile, File
from src.etl import clean_customers, validate_customers, reconcile_customers_orders,clean_orders, validate_orders
import pandas as pd
from fastapi.responses import Response

app = FastAPI()

CUSTOMERS_DF = None
ORDERS_DF = None

@app.post("/upload/customers")
async def upload_customers(file: UploadFile = File(...)):
    global CUSTOMERS_DF
    df = pd.read_csv(file.file)
    df = clean_customers(df)
    validate_customers(df)
    CUSTOMERS_DF = df
    return {"status": "success", "rows": len(df)}




@app.post("/upload/orders")
async def upload_orders(file: UploadFile = File(...)):
    global ORDERS_DF
    df = pd.read_csv(file.file)
    df = clean_orders(df)
    validate_orders(df)
    ORDERS_DF = df
    return {"status": "success", "rows": len(df)}



@app.get("/reconcile")
def reconcile(full: bool = False):
    if CUSTOMERS_DF is None or ORDERS_DF is None:
        return {"error": "Upload both customers and orders first"}

    report = reconcile_customers_orders(CUSTOMERS_DF, ORDERS_DF)

    summary = {
        "total_customers": int(report["summary"]["total_customers"]),
        "total_orders": int(report["summary"]["total_orders"]),
        "orders_without_customers": int(report["summary"]["orders_without_customers"]),
        "customers_without_orders": int(report["summary"]["customers_without_orders"]),
    }

    if not full:
        return summary

    return {
        "summary": summary,
        "orders_without_customers": report["orders_without_customers"]
            .fillna("")
            .astype(str)
            .to_dict(orient="records"),
        "customers_without_orders": report["customers_without_orders"]
            .fillna("")
            .astype(str)
            .to_dict(orient="records"),
    }


@app.get("/download/customers")
def download_customers():
    if CUSTOMERS_DF is None:
        return {"error": "No customers data uploaded"}
    csv_str = CUSTOMERS_DF.to_csv(index=False)
    return Response(content=csv_str, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=clean_customers.csv"})


@app.get("/download/orders")
def download_orders():
    if ORDERS_DF is None:
        return {"error": "No orders data uploaded"}
    
    csv_str = CUSTOMERS_DF.to_csv(index=False)
    return Response(content=csv_str, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=clean_customers.csv"})