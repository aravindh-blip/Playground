from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4
import json
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark import StorageLevel

SOURCE_LAKEHOUSE = "LH_SALES_SILVER_TEST"
SOURCE_SCHEMA = "silver_clean"
TARGET_LAKEHOUSE = "LH_SALES_GOLD_TEST"
TARGET_SCHEMA = "gold"
WRITE_GOLD = False  # First run validates only. Set True after reviewing the results.
COMPANY_KEY = "ACCREDO_PACKAGING_INC"  # Stable identifier for this one source company.
ITEM_LINE_TYPE = 2  # Business Central standard Item line type; verify for this source.
MIN_POSTING_YEAR = 1900
MAX_POSTING_YEAR = 2100

SOURCE_TABLES = [
    "sales_invoice_header", "sales_invoice_line",
    "sales_credit_memo_header", "sales_credit_memo_line",
    "customer", "item", "item_category", "salesperson_purchaser",
    "sales_header", "sales_line", "sales_line_ext",
]
OUTPUT_KEYS = {
    "dim_customer": ["company_key", "customer_key"],
    "dim_item": ["company_key", "item_key"],
    "dim_salesperson": ["company_key", "salesperson_key"],
    "dim_date": ["date_key"],
    "fact_posted_sales": ["company_key", "document_type", "document_no", "line_no"],
    "fact_open_sales_orders": ["company_key", "document_type", "document_no", "line_no"],
    "fact_shipped_not_invoiced": ["company_key", "document_type", "document_no", "line_no"],
}
