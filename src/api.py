from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import Response
import pandas as pd


from src.etl import (
    clean_customers,
    validate_customers,
    clean_orders,
    validate_orders,
    reconcile_customers_orders,
    build_reconciled_dataset,
)

app = FastAPI()

# In-memory storage for demo purposes only.
CUSTOMERS_DF = None
ORDERS_DF = None


# ----------------------------
# Helper functions
# ----------------------------

def _format_uk_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format all datetime columns to UK format (DD/MM/YYYY).
    """
    df = df.copy()
    for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
        df[col] = df[col].dt.strftime("%d/%m/%Y")
    return df


def _csv_response(df: pd.DataFrame, filename: str):
    """
    Convert a DataFrame into a downloadable CSV response.
    """
    df = _format_uk_dates(df)
    csv_str = df.to_csv(index=False)
    return Response(
        content=csv_str,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# ----------------------------
# Upload endpoints
# ----------------------------
@app.post("/upload/customers")
async def upload_customers(file: UploadFile = File(...)):
    global CUSTOMERS_DF

    if file.content_type not in ["text/csv", "application/vnd.ms-excel"]:
        raise HTTPException(
            status_code=400,
            detail="Only CSV files are supported"
        )

    try:
        df = pd.read_csv(file.file)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid CSV file")
    
    if df.empty:
        raise HTTPException(
            status_code=400,
            detail="Uploaded customers file is empty"
        )

    REQUIRED_CUSTOMER_COLUMNS = {"customer_id", "email", "signup_date"}
    missing = REQUIRED_CUSTOMER_COLUMNS - set(df.columns)

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {missing}"
        )

    try:
        df = clean_customers(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Data cleaning error: {str(e)}")
    
    validate_customers(df)

    CUSTOMERS_DF = df
    return {"status": "success", "rows": len(df)}



@app.post("/upload/orders")
async def upload_orders(file: UploadFile = File(...)):
    global ORDERS_DF

    if file.content_type not in ["text/csv", "application/vnd.ms-excel"]:
        raise HTTPException(
            status_code=400,
            detail="Only CSV files are supported"
        )

    try:
        df = pd.read_csv(file.file)
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="Invalid CSV file"
        )

    if df.empty:
        raise HTTPException(
            status_code=400,
            detail="Uploaded orders file is empty"
        )

    REQUIRED_ORDER_COLUMNS = {"order_id", "customer_id", "amount", "order_date"}
    missing = REQUIRED_ORDER_COLUMNS - set(df.columns)

    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {missing}"
        )

    try:
        df = clean_orders(df)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Data cleaning error: {str(e)}")
    
    validate_orders(df)

    ORDERS_DF = df
    return {"status": "success", "rows": len(df)}



# ----------------------------
# Reconciliation endpoints
# ----------------------------
@app.get("/reconcile")
def reconcile(full: bool = False):
    """
    Reconcile uploaded customers and orders.

    - full=False → summary only
    - full=True  → include full mismatch tables
    """
    if CUSTOMERS_DF is None or ORDERS_DF is None:
        raise HTTPException(
            status_code=400,
            detail="Upload both customers and orders first"
        )

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
        "orders_without_customers": _format_uk_dates(report["orders_without_customers"])
            .fillna("")
            .astype(str)
            .to_dict(orient="records"),
        "customers_without_orders": _format_uk_dates(report["customers_without_orders"])
            .fillna("")
            .astype(str)
            .to_dict(orient="records"),
    }


@app.get("/reconcile/combined")
def get_combined_dataset(download: bool = False):
    """
    Get the final reconciled (joined) dataset.
    """
    if CUSTOMERS_DF is None or ORDERS_DF is None:
        raise HTTPException(
            status_code=400,
            detail="Upload both customers and orders first"
        )

    combined_df = build_reconciled_dataset(CUSTOMERS_DF, ORDERS_DF)
    combined_df = _format_uk_dates(combined_df)

    if download:
        return _csv_response(combined_df, "reconciled_orders.csv")

    return combined_df.fillna("").astype(str).to_dict(orient="records")



# ----------------------------
# Download endpoints
# ----------------------------
@app.get("/download/customers")
def download_customers():
    """
    Download cleaned customers CSV.
    """
    if CUSTOMERS_DF is None:
        raise HTTPException(status_code=400, detail="No customers data uploaded")

    return _csv_response(CUSTOMERS_DF, "clean_customers.csv")


@app.get("/download/orders")
def download_orders():
    """
    Download cleaned orders CSV.
    """
    if ORDERS_DF is None:
        raise HTTPException(status_code=400, detail="No orders data uploaded")

    return _csv_response(ORDERS_DF, "clean_orders.csv")
