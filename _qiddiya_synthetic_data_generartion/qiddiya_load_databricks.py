"""
==========================================================================
Qiddiya Synthetic Data Generator — Enterprise Bronze Layer (v4.0)
DATABRICKS SQL UPLOAD (databricks-sql-connector — no SQLAlchemy dialect)
==========================================================================

Installation:
    pip install databricks-sql-connector pandas numpy faker pyarrow

Usage:
    export DATABRICKS_TOKEN="your_personal_access_token"
    python qiddiya_data_generator.py

Generation Order (Enforced):
  Phase A — Master/Reference Data:
    1. raw_staff        → staff_ids pool
    2. raw_customers    → customer_ids pool
    3. raw_products     → product_codes pool

  Phase B — Transactional Data (consumes pools):
    4. raw_transactions
    5. raw_gate_events
    6. raw_ride_operations
    7. raw_feedback
    8. raw_weather
    9. raw_master_data

Output Tables (9 per park = 18 total):
  sf_raw_customers, sf_raw_staff, sf_raw_products, sf_raw_transactions,
  sf_raw_gate_events, sf_raw_ride_operations, sf_raw_feedback,
  sf_raw_weather, sf_raw_master_data
  (and aq_ equivalents)

Destination: Databricks edw_dev.bronze
==========================================================================
"""

import os
import uuid
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker
from databricks import sql

warnings.filterwarnings("ignore")
fake = Faker()

# ── CONFIGURATION ────────────────────────────────────────────────────────

DATABRICKS_SERVER_HOSTNAME = "dbc-a66f10d7-d7e6.cloud.databricks.com"
DATABRICKS_HTTP_PATH       = "/sql/1.0/warehouses/2d8c6692f8052659"
DATABRICKS_CATALOG         = "edw_dev"
DATABRICKS_SCHEMA          = "bronze"

# Token loaded from environment variable — never hardcode credentials
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

COUNTS = {
    "transactions": 100_000,
    "gate_events":  100_000,
    "ride_ops":      20_000,
    "customers":     50_000,
    "feedback":      10_000,
    "weather":        5_000,
    "staff":          2_000,
    "master_data":    1_000,
}

DATE_START = datetime(2025, 1, 1)
DATE_END   = datetime(2026, 1, 31)
DAYS_RANGE = (DATE_END - DATE_START).days

# FK-safe columns: NEVER corrupted by data quality injection
FK_SAFE_COLUMNS = frozenset({
    "source_customer_id", "customer_id", "product_code", "cashier_id",
    "source_employee_id", "lead_operator_id", "ride_code", "ride_name",
    "gate_id", "park_code", "source_transaction_id", "source_event_id",
    "source_record_id", "source_feedback_id", "is_duplicate",
})

# Databricks dtype → SQL type mapping
DTYPE_MAP = {
    "int64":          "BIGINT",
    "int32":          "INT",
    "int16":          "SMALLINT",
    "int8":           "TINYINT",
    "float64":        "DOUBLE",
    "float32":        "FLOAT",
    "bool":           "BOOLEAN",
    "object":         "STRING",
    "string":         "STRING",
    "datetime64[ns]": "TIMESTAMP",
    "date":           "DATE",
}


# ── HELPER FUNCTIONS ─────────────────────────────────────────────────────

def get_random_dates(n_rows: int) -> np.ndarray:
    """Return n_rows random dates weighted for KSA weekends (Fri/Sat)."""
    all_dates = [DATE_START + timedelta(days=x) for x in range(DAYS_RANGE)]
    weights   = [1.3 if d.weekday() in (4, 5) else 1.0 for d in all_dates]
    total_w   = sum(weights)
    probs     = [w / total_w for w in weights]
    return np.random.choice(all_dates, size=n_rows, p=probs)


def generate_ids(prefix: str, n_rows: int) -> list:
    """Generate zero-padded sequential IDs: PREFIX-0000001 … PREFIX-NNNNNNN."""
    return [f"{prefix}-{i:07d}" for i in range(1, n_rows + 1)]


def batch_id() -> str:
    return f"BATCH-{uuid.uuid4().hex[:8].upper()}"


def inject_data_quality_issues(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Inject controlled data quality issues while PROTECTING FK columns.
    """
    n = len(df)

    # 1. Malformed transaction timestamps
    if "transaction_datetime" in df.columns:
        idx = np.random.choice(n, size=int(n * 0.02), replace=False)
        df.loc[idx, "transaction_datetime"] = "Invalid-Date-String"

    # 2. Duplicate rows (~1%)
    n_dupes = int(n * 0.01)
    if n_dupes > 0:
        dupes = df.sample(n_dupes, random_state=42).copy()
        dupes["is_duplicate"] = True
        df = pd.concat([df, dupes], ignore_index=True)

    # 3. Boolean format inconsistencies (FK-safe columns excluded)
    bool_cols = [
        c for c in df.columns
        if ("is_" in c or "flag" in c) and c not in FK_SAFE_COLUMNS
    ]
    for col in bool_cols:
        mask = np.random.rand(len(df))
        df.loc[mask < 0.20,                     col] = "1"
        df.loc[(mask >= 0.20) & (mask < 0.40),  col] = "true"
        df.loc[(mask >= 0.40) & (mask < 0.60),  col] = "Y"
        df.loc[(mask >= 0.60) & (mask < 0.80),  col] = "yes"

    # 4. Ensure is_duplicate column always exists
    if "is_duplicate" not in df.columns:
        df["is_duplicate"] = False

    return df


def transform_to_aq(sf_df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive AQ dataset by replacing 'SF-' prefix with 'AQ-' in all string columns.
    park_code is handled explicitly ('SF' → 'AQ').
    """
    print("    Transforming SF → AQ...")
    aq_df = sf_df.copy()

    for col in aq_df.select_dtypes(include=["object", "string"]).columns:
        non_null = aq_df[col].notna()
        if non_null.any():
            aq_df.loc[non_null, col] = (
                aq_df.loc[non_null, col]
                .astype(str)
                .str.replace("SF-", "AQ-", regex=False)
            )
            aq_df[col] = aq_df[col].replace({"None": None, "nan": None, "NaN": None})

    if "park_code" in aq_df.columns:
        aq_df["park_code"] = "AQ"

    return aq_df


# ── DATA GENERATOR ───────────────────────────────────────────────────────

class QiddiyaDataGenerator:
    """
    Generates synthetic Bronze-layer data with enforced referential integrity.
    """

    def __init__(self):
        self.customer_ids:    list = []
        self.staff_ids:       list = []
        self.product_codes:   list = []
        self.product_pool_df: pd.DataFrame = pd.DataFrame()

        self.ride_pool = [
            ("SF-RIDE-01", "Falcon Flight"),
            ("SF-RIDE-02", "Sirocco Tower"),
            ("SF-RIDE-03", "Spitfire"),
            ("SF-RIDE-04", "Iron Rattler"),
        ]
        self.gate_pool      = [f"G-{i:02d}" for i in range(1, 20)]
        self.gate_locations = ["Main Entrance", "VIP Entrance", "Staff Gate"]
        self.zone_codes     = ["Z01", "Z02", "Z03"]

    # ── Master Tables ────────────────────────────────────────────────────

    def generate_staff(self, n_rows: int) -> pd.DataFrame:
        print(f"  [1/9] Generating {n_rows:,} Staff (master)...")
        ids = [f"SF-EMP-{i:04d}" for i in range(1, n_rows + 1)]
        self.staff_ids = ids

        df = pd.DataFrame({
            "source_employee_id":  ids,
            "department":          np.random.choice(["Ops", "Security", "F&B", "Retail"], n_rows),
            "nationality":         np.random.choice(["Saudi", "Expat"], n_rows, p=[0.7, 0.3]),
            "is_active":           ["1"] * n_rows,
            "ingestion_timestamp": pd.Timestamp.now(),
            "ingestion_date":      pd.to_datetime("today").date(),
            "batch_id":            batch_id(),
            "pipeline_name":       "hr_to_bronze",
            "validation_status":   "PENDING",
        })
        return df

    def generate_customers(self, n_rows: int) -> pd.DataFrame:
        print(f"  [2/9] Generating {n_rows:,} Customers (master)...")
        ids = generate_ids("SF-CUST", n_rows)
        self.customer_ids = ids

        df = pd.DataFrame({
            "source_customer_id": ids,
            "source_system":      ["CRM_Dynamics"] * n_rows,
            "full_name":          [fake.name() for _ in range(n_rows)],
            "gender":             np.random.choice(["M", "F"], n_rows),
            "date_of_birth": [
                fake.date_of_birth(minimum_age=5, maximum_age=90).isoformat()
                for _ in range(n_rows)
            ],
            "nationality": np.random.choice(
                ["Saudi", "UAE", "Kuwait", "UK", "USA", "India", "Philippines"],
                n_rows, p=[0.6, 0.1, 0.05, 0.05, 0.05, 0.1, 0.05],
            ),
            "email":         [fake.email()        for _ in range(n_rows)],
            "phone":         [fake.phone_number() for _ in range(n_rows)],
            "resident_type": np.random.choice(["Citizen", "Resident", "Tourist"], n_rows),
            "loyalty_tier":  np.random.choice(
                ["None", "Silver", "Gold", "Platinum"],
                n_rows, p=[0.7, 0.2, 0.08, 0.02],
            ),
            "registration_date": [
                d.strftime("%Y-%m-%d") for d in get_random_dates(n_rows)
            ],
        })

        df[["first_name", "last_name"]] = (
            df["full_name"].str.split(" ", n=1, expand=True)
        )
        df["loyalty_card_number"] = df["source_customer_id"].str.replace("CUST", "LC")
        df["loyalty_points"]      = np.random.randint(0, 5000, n_rows)
        df["app_installed"]       = np.random.choice(["True", "False"], n_rows)
        df["email_opt_in"]        = np.random.choice(["Y", "N"], n_rows)
        df["sms_opt_in"]          = np.random.choice(["Y", "N"], n_rows)
        df["cdc_operation"]       = "INSERT"
        df["record_created_at"]   = pd.Timestamp.now()
        df["validation_status"]   = "PENDING"
        df["ingestion_date"]      = pd.to_datetime("today").date()
        df["ingestion_timestamp"] = pd.Timestamp.now()
        df["batch_id"]            = batch_id()
        df["pipeline_name"]       = "crm_to_bronze"
        return df

    def generate_products(self) -> pd.DataFrame:
        print("  [3/9] Generating Products (master)...")
        categories = ["ADMISSION", "F&B", "MERCH", "FASTPASS"]
        rows = [
            {
                "product_code":        f"SF-SKU-{i:04d}",
                "description":         f"{np.random.choice(categories)} Item {i}",
                "category":            np.random.choice(categories),
                "price":               round(float(np.random.uniform(10, 500)), 2),
                "ingestion_timestamp": pd.Timestamp.now(),
                "ingestion_date":      pd.to_datetime("today").date(),
                "batch_id":            batch_id(),
                "pipeline_name":       "erp_to_bronze",
                "validation_status":   "PENDING",
            }
            for i in range(100)
        ]
        self.product_pool_df = pd.DataFrame(rows)
        self.product_codes   = self.product_pool_df["product_code"].tolist()
        return self.product_pool_df

    # ── Transactional Tables ─────────────────────────────────────────────

    def generate_transactions(self, n_rows: int) -> pd.DataFrame:
        print(f"  [4/9] Generating {n_rows:,} Transactions...")
        assert self.customer_ids,  "FATAL: customer_ids pool empty!"
        assert self.product_codes, "FATAL: product_codes pool empty!"
        assert self.staff_ids,     "FATAL: staff_ids pool empty!"

        dates = get_random_dates(n_rows)

        cust_choices = np.random.choice(self.customer_ids, n_rows)
        is_anonymous = np.random.random(n_rows) < 0.05
        customer_ids = [
            None if anon else cid
            for cid, anon in zip(cust_choices, is_anonymous)
        ]

        prod_idx   = np.random.randint(0, len(self.product_pool_df), n_rows)
        prods      = self.product_pool_df.iloc[prod_idx].reset_index(drop=True)
        quantity   = np.random.randint(1, 6, n_rows)
        unit_price = prods["price"].values.astype(float)
        gross      = np.round(quantity * unit_price, 2)
        discount   = np.where(np.random.random(n_rows) < 0.1, np.round(gross * 0.1, 2), 0.0)
        net        = np.round(gross - discount, 2)
        vat        = np.round(net * 0.15, 2)
        total      = np.round(net + vat, 2)

        df = pd.DataFrame({
            "source_transaction_id": [
                f"SF-TXN-{d.strftime('%Y%m%d')}-{i:06d}"
                for i, d in enumerate(dates, start=1)
            ],
            "source_system":       ["POS_Oracle"]       * n_rows,
            "source_table":        ["pos_sales_lines"]  * n_rows,
            "park_code":           ["SF"]               * n_rows,
            "terminal_id": [
                f"POS-GATE-{np.random.randint(1, 50):02d}" for _ in range(n_rows)
            ],
            "cashier_id":           np.random.choice(self.staff_ids, n_rows),
            "customer_id":          customer_ids,
            "transaction_datetime": [d.isoformat() for d in dates],
            "product_code":         prods["product_code"].values,
            "product_description":  prods["description"].values,
            "product_category":     prods["category"].values,
            "unit_price":           unit_price,
            "quantity":             quantity,
            "payment_method":       np.random.choice(["CreditCard", "Cash", "ApplePay"], n_rows),
            "transaction_status":   np.random.choice(
                ["SUCCESS", "FAILED", "REFUNDED"], n_rows, p=[0.95, 0.02, 0.03]
            ),
            "sales_channel":        np.random.choice(["Gate", "Online", "App"], n_rows),
            "is_group_booking":     np.random.choice(["Y", "N"], n_rows, p=[0.1, 0.9]),
            "group_size":           np.random.randint(1, 11, n_rows),
            "booking_reference": [
                f"BKG-{d.year}-{uuid.uuid4().hex[:4].upper()}" for d in dates
            ],
            "gross_amount":   gross,
            "discount_amount": discount,
            "net_amount":      net,
            "vat_amount":      vat,
            "total_charged":   total,
        })

        df["refund_reference_id"] = np.where(
            df["transaction_status"] == "REFUNDED",
            [f"REF-{uuid.uuid4().hex[:8]}" for _ in range(n_rows)],
            None,
        )
        df["cdc_operation"]       = "INSERT"
        df["ingestion_timestamp"] = pd.Timestamp.now()
        df["ingestion_date"]      = pd.to_datetime("today").date()
        df["batch_id"]            = batch_id()
        df["pipeline_name"]       = "pos_to_bronze"
        df["validation_status"]   = "PENDING"
        return df

    def generate_gate_events(self, n_rows: int) -> pd.DataFrame:
        print(f"  [5/9] Generating {n_rows:,} Gate Events...")
        assert self.customer_ids, "FATAL: customer_ids pool empty!"

        dates = get_random_dates(n_rows)

        df = pd.DataFrame({
            "source_event_id": [f"SF-EVT-{uuid.uuid4().hex[:8]}" for _ in range(n_rows)],
            "source_system":   ["Turnstile_System"] * n_rows,
            "park_code":       ["SF"]               * n_rows,
            "gate_id":         np.random.choice(self.gate_pool,      n_rows),
            "gate_location":   np.random.choice(self.gate_locations, n_rows),
            "zone_code":       np.random.choice(self.zone_codes,     n_rows),
            "event_type":      np.random.choice(["ENTRY", "EXIT"], n_rows, p=[0.8, 0.2]),
            "customer_id":     np.random.choice(self.customer_ids,  n_rows),
            "event_datetime":  [d.isoformat() for d in dates],
            "ticket_barcode": [
                f"TKT-{uuid.uuid4().hex[:12].upper()}" for _ in range(n_rows)
            ],
            "validation_status": ["PENDING"] * n_rows,
        })

        df["rejection_reason"] = np.where(
            np.random.random(n_rows) < 0.01, "INVALID_TICKET", None
        )
        df["ingestion_timestamp"] = pd.Timestamp.now()
        df["ingestion_date"]      = pd.to_datetime("today").date()
        df["batch_id"]            = batch_id()
        df["pipeline_name"]       = "gate_to_bronze"
        return df

    def generate_ride_ops(self, n_rows: int) -> pd.DataFrame:
        print(f"  [6/9] Generating {n_rows:,} Ride Operations...")
        assert self.staff_ids, "FATAL: staff_ids pool empty!"
        assert self.ride_pool, "FATAL: ride_pool empty!"

        dates    = get_random_dates(n_rows)
        ride_idx = np.random.randint(0, len(self.ride_pool), n_rows)

        df = pd.DataFrame({
            "source_record_id": [f"SF-OPS-{uuid.uuid4().hex[:8]}" for _ in range(n_rows)],
            "source_system":    ["Ride_SCADA"] * n_rows,
            "park_code":        ["SF"]          * n_rows,
            "ride_code":        [self.ride_pool[i][0] for i in ride_idx],
            "ride_name":        [self.ride_pool[i][1] for i in ride_idx],
            "poll_datetime":    [d.isoformat() for d in dates],
            "operational_status": np.random.choice(
                ["OPERATIONAL", "MAINTENANCE", "CLOSED"], n_rows, p=[0.9, 0.05, 0.05]
            ),
            "current_queue_length":    np.random.randint(0, 150, n_rows),
            "estimated_wait_min":      np.random.randint(0, 120, n_rows),
            "riders_last_cycle":       np.random.randint(0,  30, n_rows),
            "safety_interlock_active": np.random.choice(["true", "false"], n_rows),
            "lead_operator_id":        np.random.choice(self.staff_ids, n_rows),
        })

        df["ingestion_timestamp"] = pd.Timestamp.now()
        df["ingestion_date"]      = pd.to_datetime("today").date()
        df["batch_id"]            = batch_id()
        df["pipeline_name"]       = "scada_to_bronze"
        df["validation_status"]   = "PENDING"
        return df

    def generate_feedback(self, n_rows: int) -> pd.DataFrame:
        print(f"  [7/9] Generating {n_rows:,} Feedback rows...")
        assert self.customer_ids, "FATAL: customer_ids pool empty!"

        dates              = get_random_dates(n_rows)
        has_complaint_flags = np.random.random(n_rows) < 0.1

        df = pd.DataFrame({
            "source_feedback_id": [f"SF-FBK-{uuid.uuid4().hex[:8]}" for _ in range(n_rows)],
            "park_code":          ["SF"] * n_rows,
            "customer_id":        np.random.choice(self.customer_ids, n_rows),
            "feedback_date":      [d.strftime("%Y-%m-%d") for d in dates],
            "overall_score":      np.random.randint(1, 11, n_rows),
            "nps_raw":            np.random.randint(0, 11, n_rows),
            "cleanliness_score":  np.random.randint(1,  6, n_rows),
            "staff_score":        np.random.randint(1,  6, n_rows),
            "has_complaint": ["true" if f else "false" for f in has_complaint_flags],
            "complaint_text": [
                fake.sentence() if f else None for f in has_complaint_flags
            ],
            "ingestion_timestamp": pd.Timestamp.now(),
            "ingestion_date":      pd.to_datetime("today").date(),
            "batch_id":            batch_id(),
            "pipeline_name":       "feedback_to_bronze",
            "validation_status":   "PENDING",
        })
        return df

    def generate_weather(self, n_rows: int) -> pd.DataFrame:
        print(f"  [8/9] Generating {n_rows:,} Weather rows...")
        dates = get_random_dates(n_rows)

        df = pd.DataFrame({
            "park_code":          ["SF"] * n_rows,
            "poll_datetime":      [d.isoformat() for d in dates],
            "temperature_c":      np.round(np.random.uniform(20, 50, n_rows), 1),
            "feels_like_c":       np.round(np.random.uniform(20, 55, n_rows), 1),
            "temperature_min_c":  np.round(np.random.uniform(15, 30, n_rows), 1),
            "temperature_max_c":  np.round(np.random.uniform(35, 55, n_rows), 1),
            "humidity_pct":       np.random.randint(10, 90, n_rows),
            "wind_speed_ms":      np.round(np.random.uniform(0, 15, n_rows), 1),
            "wind_direction_deg": np.random.randint(0, 360, n_rows),
            "uv_index":           np.random.randint(0, 11, n_rows),
            "visibility_m":       np.random.randint(5000, 10000, n_rows),
            "rainfall_1h_mm": np.round(
                np.where(
                    np.random.random(n_rows) < 0.05,
                    np.random.uniform(0, 10, n_rows),
                    0.0,
                ), 1
            ),
            "weather_code": np.random.choice(
                ["CLEAR", "CLOUDY", "RAIN", "SANDSTORM"], n_rows
            ),
            "weather_description": "Normal",
            "air_quality_index":   np.random.randint(1, 5, n_rows),
            "http_status_code":    "200",
            "api_error_message":   None,
            "ingestion_timestamp": pd.Timestamp.now(),
            "ingestion_date":      pd.to_datetime("today").date(),
            "batch_id":            batch_id(),
            "pipeline_name":       "weather_to_bronze",
            "validation_status":   "PENDING",
        })
        return df

    def generate_master_data(self, n_rows: int) -> pd.DataFrame:
        print(f"  [9/9] Generating {n_rows:,} Master Data rows...")
        df = pd.DataFrame({
            "source_record_id": [f"SF-MST-{i:04d}" for i in range(1, n_rows + 1)],
            "source_system":    ["ERP"] * n_rows,
            "record_type":      np.random.choice(["SKU", "ASSET", "LOCATION"], n_rows),
            "price_amount":     np.round(np.random.uniform(10, 1000, n_rows), 2),
            "ingestion_timestamp": pd.Timestamp.now(),
            "ingestion_date":      pd.to_datetime("today").date(),
            "batch_id":            batch_id(),
            "pipeline_name":       "erp_to_bronze",
            "validation_status":   "PENDING",
        })
        return df


# ── REFERENTIAL INTEGRITY VALIDATOR ──────────────────────────────────────

def validate_referential_integrity(data_map: dict) -> bool:
    print("\n" + "=" * 70)
    print("  REFERENTIAL INTEGRITY VALIDATION REPORT")
    print("=" * 70)

    cust_ids   = set(data_map["customers"]["source_customer_id"])
    staff_ids  = set(data_map["staff"]["source_employee_id"])
    prod_codes = set(data_map["products"]["product_code"])

    checks = []

    def _check(label, df, col, valid_set, nullable=False):
        target = df.copy()
        if nullable:
            target = target[target[col].notna() & (target[col] != "None")]
        orphans = target[~target[col].isin(valid_set)]
        checks.append({
            "label":    label,
            "total":    len(target),
            "orphans":  len(orphans),
            "nullable": nullable,
        })

    txn  = data_map["transactions"]
    gate = data_map["gate_events"]
    ride = data_map["ride_ops"]
    fb   = data_map["feedback"]

    _check("transactions.customer_id   → customers.source_customer_id",
           txn,  "customer_id",      cust_ids,  nullable=True)
    _check("transactions.product_code  → products.product_code",
           txn,  "product_code",     prod_codes)
    _check("transactions.cashier_id    → staff.source_employee_id",
           txn,  "cashier_id",       staff_ids)
    _check("gate_events.customer_id    → customers.source_customer_id",
           gate, "customer_id",      cust_ids)
    _check("ride_ops.lead_operator_id  → staff.source_employee_id",
           ride, "lead_operator_id", staff_ids)
    _check("feedback.customer_id       → customers.source_customer_id",
           fb,   "customer_id",      cust_ids)

    all_pass = True
    for r in checks:
        pct    = r["orphans"] / max(r["total"], 1) * 100
        status = "✅ PASS" if r["orphans"] == 0 else "❌ FAIL"
        tag    = " (nullable)" if r["nullable"] else ""
        if r["orphans"] > 0:
            all_pass = False
        print(f"  {status} | {r['label']}{tag}")
        print(f"         Total: {r['total']:,} | Orphans: {r['orphans']:,} ({pct:.2f}%)")

    print("-" * 70)
    if all_pass:
        print("  ✅ ALL CHECKS PASSED — 100% Referential Integrity Confirmed")
    else:
        print("  ❌ INTEGRITY VIOLATIONS DETECTED — Review above failures")
    print("=" * 70)
    return all_pass


# ── DATABRICKS UPLOADER ───────────────────────────────────────────────────

def _infer_ddl(df: pd.DataFrame) -> str:
    """Build a CREATE TABLE column definition string from a DataFrame."""
    parts = []
    for col, dtype in zip(df.columns, df.dtypes):
        sql_type = DTYPE_MAP.get(str(dtype), "STRING")
        parts.append(f"`{col}` {sql_type}")
    return ", ".join(parts)


def _sanitize_value(v):
    """Convert Python/numpy scalars to types safe for Databricks parameterised queries."""
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return str(v)
    if hasattr(v, "isoformat"):        # date / datetime
        return str(v)
    return v


def _format_sql_value(v):
    """Format sanitized value into Databricks SQL string syntax."""
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def upload_dataframe(df: pd.DataFrame, table_name: str) -> bool:
    """
    Upload a DataFrame to Databricks using databricks-sql-connector.

    Strategy:
      1. DROP TABLE IF EXISTS
      2. CREATE TABLE with inferred DDL
      3. INSERT rows via executemany in chunks of 1 000

    No SQLAlchemy dialect required.
    """
    if df is None or len(df) == 0:
        print(f"  ⚠  Skipped {table_name} — empty DataFrame.")
        return True

    if not DATABRICKS_TOKEN:
        print("  ✗  DATABRICKS_TOKEN environment variable is not set. Aborting upload.")
        return False

    # Add surrogate bronze_id as the first column
    upload = df.copy()
    upload.insert(0, "bronze_id", range(1, len(upload) + 1))

    full_table   = f"`{DATABRICKS_CATALOG}`.`{DATABRICKS_SCHEMA}`.`{table_name}`"
    cols_ddl     = _infer_ddl(upload)
    n_cols       = len(upload.columns)
    placeholders = ", ".join(["?"] * n_cols)
    chunk_size   = 1_000

    print(
        f"\n  Uploading {table_name} "
        f"({len(upload):,} rows, {n_cols} cols) → {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}..."
    )

    try:
        with sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"USE CATALOG `{DATABRICKS_CATALOG}`")
                cursor.execute(f"USE SCHEMA `{DATABRICKS_SCHEMA}`")

                # Recreate table
                cursor.execute(f"DROP TABLE IF EXISTS {full_table}")
                cursor.execute(f"CREATE TABLE {full_table} ({cols_ddl})")

                # Chunked insert
                chunk_size   = 2_000
                total_chunks = (len(upload) + chunk_size - 1) // chunk_size
                for chunk_num, start in enumerate(range(0, len(upload), chunk_size), 1):
                    chunk = upload.iloc[start : start + chunk_size]
                    
                    val_strings = []
                    for row in chunk.itertuples(index=False, name=None):
                        row_vals = [_format_sql_value(_sanitize_value(v)) for v in row]
                        val_strings.append("(" + ", ".join(row_vals) + ")")
                        
                    val_block = ", ".join(val_strings)
                    cursor.execute(f"INSERT INTO {full_table} VALUES {val_block}")
                    
                    if chunk_num % 5 == 0 or chunk_num == total_chunks:
                        print(
                            f"    chunk {chunk_num}/{total_chunks} "
                            f"({min(start + chunk_size, len(upload)):,}/{len(upload):,} rows)"
                        )

        print(f"  ✓  {table_name} uploaded successfully.")
        return True

    except Exception as exc:
        print(f"  ✗  Error uploading {table_name}: {exc}")
        return False


# ── MAIN ─────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  Qiddiya Synthetic Data Generator v4.0")
    print(f"  Target : Databricks {DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}")
    print(f"  Token  : {'SET ✓' if DATABRICKS_TOKEN else 'NOT SET ✗  (export DATABRICKS_TOKEN=...)'}")
    print("=" * 70)

    if not DATABRICKS_TOKEN:
        print("\n  FATAL: DATABRICKS_TOKEN is not set. Cannot proceed.")
        print("  Run:  export DATABRICKS_TOKEN='your_personal_access_token'")
        return

    gen = QiddiyaDataGenerator()

    # ── Phase 1: Master tables (populate pools) ───────────────────────────
    print("\n[Phase 1] Generating SF Master Data...")
    data_map = {
        "staff":     gen.generate_staff(COUNTS["staff"]),
        "customers": gen.generate_customers(COUNTS["customers"]),
        "products":  gen.generate_products(),
    }

    # ── Phase 2: Transactional tables ─────────────────────────────────────
    print("\n[Phase 2] Generating SF Transactional Data...")
    data_map["transactions"] = gen.generate_transactions(COUNTS["transactions"])
    data_map["gate_events"]  = gen.generate_gate_events(COUNTS["gate_events"])
    data_map["ride_ops"]     = gen.generate_ride_ops(COUNTS["ride_ops"])
    data_map["feedback"]     = gen.generate_feedback(COUNTS["feedback"])
    data_map["weather"]      = gen.generate_weather(COUNTS["weather"])
    data_map["master_data"]  = gen.generate_master_data(COUNTS["master_data"])

    # ── Phase 3: Baseline integrity check ─────────────────────────────────
    print("\n[Phase 3] Pre-Anomaly Referential Integrity Check...")
    if not validate_referential_integrity(data_map):
        print("\n  ⚠️  CRITICAL: Baseline integrity failed. Aborting.")
        return

    # ── Phase 4: Save clean copy BEFORE anomaly injection ─────────────────
    clean_data_map = {k: v.copy() for k, v in data_map.items()}

    # ── Phase 5: Inject SF anomalies ──────────────────────────────────────
    print("\n[Phase 4] Injecting SF Anomalies (FK columns protected)...")
    for key in data_map:
        data_map[key] = inject_data_quality_issues(data_map[key], key)

    # ── Phase 6: Post-anomaly integrity check ─────────────────────────────
    print("\n[Phase 5] Post-Anomaly Referential Integrity Check...")
    if not validate_referential_integrity(data_map):
        print("\n  ⚠️  CRITICAL: Anomaly injection corrupted FK integrity. Aborting.")
        return

    # ── Phase 7: Transform clean copy → AQ ────────────────────────────────
    print("\n[Phase 6] Generating AQ Data (from clean SF copy)...")
    aq_data_map = {key: transform_to_aq(df) for key, df in clean_data_map.items()}

    # ── Phase 8: Inject AQ anomalies + validate ───────────────────────────
    print("\n[Phase 7] Injecting AQ Anomalies...")
    for key in aq_data_map:
        aq_data_map[key] = inject_data_quality_issues(aq_data_map[key], key)

    print("\n[Phase 8] AQ Referential Integrity Check...")
    if not validate_referential_integrity(aq_data_map):
        print("\n  ⚠️  CRITICAL: AQ integrity failed. Aborting.")
        return

    # ── Phase 9: Upload all 18 tables ─────────────────────────────────────
    print("\n[Phase 9] Uploading to Databricks...")

    table_mapping = [
        # (data_map key,  Databricks table suffix)
        ("staff",        "raw_staff"),
        ("customers",    "raw_customers"),
        ("products",     "raw_products"),
        ("transactions", "raw_transactions"),
        ("gate_events",  "raw_gate_events"),
        ("ride_ops",     "raw_ride_operations"),
        ("feedback",     "raw_feedback"),
        ("weather",      "raw_weather"),
        ("master_data",  "raw_master_data"),
    ]

    results: dict[str, bool] = {}

    for key, table_suffix in table_mapping:
        sf_name = f"sf_{table_suffix}"
        aq_name = f"aq_{table_suffix}"
        results[sf_name] = upload_dataframe(data_map[key],    sf_name)
        results[aq_name] = upload_dataframe(aq_data_map[key], aq_name)

    # ── Summary ────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    passed = [t for t, ok in results.items() if ok]
    failed = [t for t, ok in results.items() if not ok]

    if failed:
        print(f"  ❌ PIPELINE FAILED — {len(failed)}/{len(results)} table(s) errored:")
        for t in failed:
            print(f"     • {t}")
    else:
        print(
            f"  ✅ PIPELINE COMPLETE — All {len(results)} tables uploaded to "
            f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA} with 100% referential integrity"
        )
    print("=" * 70)


if __name__ == "__main__":
    main()
