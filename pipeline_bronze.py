"""
============================================================================
FILE: bronze_pipeline.py
============================================================================

OBJECTIVE:
    Build the Bronze Layer of our Medallion Architecture.
    Read raw JSON/CSV files → validate → split into clean vs quarantine tables.

DATA FLOW:
    data/raw/  →  [Schema Validation]  →  delta/bronze_funnel_logs     (clean)
                                       →  delta/quarantine_invalid_logs (bad)

KEY CONCEPTS:
    - Medallion Architecture: Bronze = raw ingestion, minimal transformation
    - Schema-on-Read: enforce expected column types at ingestion time
    - Data Quality Gate: reject rows missing critical fields before they
      pollute downstream analytics

INTERVIEW QUESTIONS:
    1. What is the Bronze layer in Medallion Architecture?
       → Raw data ingestion with minimal changes. Source-of-truth copy.
    2. Why quarantine bad records instead of dropping them?
       → For auditing, debugging, and data quality reporting.
    3. What is schema-on-read?
       → Defining expected schema when reading, not when storing.
    4. Why use Delta format instead of Parquet?
       → Delta adds ACID transactions, time travel, and schema evolution.
============================================================================
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ──────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────

RAW_DIR = "data/raw"
BRONZE_TABLE = "delta/bronze_funnel_logs"
QUARANTINE_TABLE = "delta/quarantine_invalid_logs"

# Fields that MUST be present and non-empty for a row to be valid
CRITICAL_FIELDS = ["session_id", "timestamp"]


# ──────────────────────────────────────────────────────────────────────────
# SCHEMAS — Define expected structure for each data source
# ──────────────────────────────────────────────────────────────────────────

CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("page_url", StringType()),
    StructField("action", StringType()),
])

CHECKOUT_SCHEMA = StructType([
    StructField("checkout_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("course_id", StringType()),
    StructField("course_name", StringType()),
    StructField("price_inr", IntegerType()),
    StructField("step_reached", StringType()),
    StructField("timestamp", StringType()),
])

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("checkout_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount_paid", IntegerType()),
    StructField("payment_status", StringType()),
    StructField("timestamp", StringType()),
])


# ──────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ──────────────────────────────────────────────────────────────────────────

def get_spark():
    """Create a local Spark session with Delta Lake support."""
    from delta import configure_spark_with_delta_pip

    # Set HADOOP_HOME for Windows (needed for winutils.exe)
    hadoop_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    builder = (
        SparkSession.builder
        .appName("EduMetrics_Bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ──────────────────────────────────────────────────────────────────────────
# STEP 1: READ RAW DATA
# Each source has a different format (JSON / CSV) and schema.
# We tag each record with its event_type so they coexist in one table.
# ──────────────────────────────────────────────────────────────────────────

def read_all_sources(spark):
    """Read all three raw data sources and union them into one DataFrame."""
    # Find the data directory (walk to find the partition folder)
    data_dirs = []
    for root, dirs, files in os.walk(RAW_DIR):
        if files:
            data_dirs.append(root)
    if not data_dirs:
        raise FileNotFoundError(f"No data files found in {RAW_DIR}/")

    data_dir = data_dirs[0]  # Use the first partition found
    dfs = []

    # --- Clickstream (JSON array → needs multiLine) ---
    click_path = os.path.join(data_dir, "clickstream_events.json")
    if os.path.exists(click_path):
        df = spark.read.schema(CLICKSTREAM_SCHEMA).option("multiLine", True).json(click_path)
        df = df.withColumn("event_type", F.lit("clickstream"))
        dfs.append(df)
        print(f"  📥 Clickstream: {df.count():,} rows")

    # --- Checkouts (JSON) ---
    checkout_path = os.path.join(data_dir, "checkout_attempts.json")
    if os.path.exists(checkout_path):
        df = spark.read.schema(CHECKOUT_SCHEMA).option("multiLine", True).json(checkout_path)
        df = df.withColumn("event_type", F.lit("checkout"))
        dfs.append(df)
        print(f"  📥 Checkouts  : {df.count():,} rows")

    # --- Orders (CSV) ---
    order_path = os.path.join(data_dir, "successful_orders.csv")
    if os.path.exists(order_path):
        df = spark.read.schema(ORDER_SCHEMA).option("header", True).csv(order_path)
        df = df.withColumn("event_type", F.lit("order"))
        dfs.append(df)
        print(f"  📥 Orders     : {df.count():,} rows")

    if not dfs:
        raise FileNotFoundError("No data files found!")

    # Normalize columns — add missing columns as null so union works
    all_cols = set()
    for df in dfs:
        all_cols.update(df.columns)

    normalized = []
    for df in dfs:
        for col in all_cols:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None).cast(StringType()))
        normalized.append(df.select(sorted(all_cols)))

    # Union all sources into one DataFrame
    combined = normalized[0]
    for df in normalized[1:]:
        combined = combined.unionByName(df)

    return combined


# ──────────────────────────────────────────────────────────────────────────
# STEP 2: VALIDATE & SPLIT
# Rows with null/empty session_id OR timestamp → quarantine
# Everything else → bronze (clean)
# ──────────────────────────────────────────────────────────────────────────

def validate_and_split(df):
    """Split DataFrame into valid and invalid partitions.
    
    Rules:
    - Clickstream & Checkout: session_id AND timestamp must be present
    - Orders: only timestamp is required (orders don't have session_id)
    """
    # timestamp must be non-null and non-empty for ALL event types
    ts_ok = F.col("timestamp").isNotNull() & (F.trim(F.col("timestamp")) != "")
    
    # session_id required only for clickstream and checkout (not orders)
    sid_ok = (
        (F.col("event_type") == "order")  # orders don't need session_id
        | (F.col("session_id").isNotNull() & (F.trim(F.col("session_id")) != ""))
    )
    
    is_valid = ts_ok & sid_ok
    valid_df = df.filter(is_valid)
    invalid_df = df.filter(~is_valid)

    return valid_df, invalid_df


# ──────────────────────────────────────────────────────────────────────────
# STEP 3: WRITE TO DELTA TABLES
# ──────────────────────────────────────────────────────────────────────────

def write_delta(df, path, label):
    """Write a DataFrame to a Delta table."""
    count = df.count()
    if count > 0:
        df.write.format("delta").mode("overwrite").save(path)
        print(f"  ✅ {label}: {count:,} rows → {path}")
    else:
        print(f"  ⚠️  {label}: 0 rows (skipped)")
    return count


# ──────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────────────────

def run_bronze_pipeline():
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("=" * 60)
    print("🔶 Bronze Pipeline — Raw Ingestion & Validation")
    print("=" * 60)

    spark = get_spark()

    # Step 1: Read raw data
    print("\n📂 Reading raw data sources...")
    combined_df = read_all_sources(spark)
    total = combined_df.count()
    print(f"  📊 Total combined: {total:,} rows")

    # Step 2: Validate
    print("\n🔍 Validating critical fields:", CRITICAL_FIELDS)
    valid_df, invalid_df = validate_and_split(combined_df)

    # Step 3: Write Delta tables
    print("\n💾 Writing Delta tables...")
    clean = write_delta(valid_df, BRONZE_TABLE, "Bronze (clean)")
    quarantined = write_delta(invalid_df, QUARANTINE_TABLE, "Quarantine (bad)")

    # Summary
    print(f"\n📈 Pipeline Summary:")
    print(f"  Total ingested   : {total:,}")
    print(f"  Clean records    : {clean:,} ({clean/total*100:.1f}%)")
    print(f"  Quarantined      : {quarantined:,} ({quarantined/total*100:.1f}%)")

    spark.stop()

if __name__ == "__main__":
    run_bronze_pipeline()

"""
SAMPLE OUTPUT:
============================================================
🔶 Bronze Pipeline — Raw Ingestion & Validation
============================================================

📂 Reading raw data sources...
  📥 Clickstream: 1,000 rows
  📥 Checkouts  : 250 rows
  📥 Orders     : 58 rows
  📊 Total combined: 1,308 rows

🔍 Validating critical fields: ['session_id', 'timestamp']

💾 Writing Delta tables...
  ✅ Bronze (clean): 1,246 rows → delta/bronze_funnel_logs
  ✅ Quarantine (bad): 62 rows  → delta/quarantine_invalid_logs

📈 Pipeline Summary:
  Total ingested   : 1,308
  Clean records    : 1,246 (95.3%)
  Quarantined      : 62 (4.7%)
"""
