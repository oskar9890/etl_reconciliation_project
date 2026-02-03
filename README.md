# Customer & Order Reconciliation API

A FastAPI-based ETL-style service for cleaning, validating, and reconciling customer and order datasets.  
The API identifies data quality issues and mismatches between systems and exposes both summary and detailed reconciliation results.

---

## Problem

Customer and order data often come from different sources and contain inconsistencies such as:

- Missing or duplicate IDs
- Invalid email formats or dates
- Orders referencing non-existent customers
- Customers with no associated orders

These issues make reporting and analytics unreliable.  
This project provides a simple, testable API to standardize, validate, and reconcile these datasets.

---

## Architecture

The project follows an ETL-style design:

### Extract
- CSV files are uploaded via FastAPI endpoints

### Transform
- Data is cleaned and normalized using pandas
- Invalid values are coerced and flagged instead of silently dropped

### Validate
- Explicit validation rules enforce data integrity
- Clear errors are raised for unrecoverable issues (e.g. duplicates)

### Serve
- Reconciliation results are exposed via API endpoints
- Cleaned datasets can be downloaded as CSV

Validation and transformation logic is separated from the API layer to support reuse and testing.



