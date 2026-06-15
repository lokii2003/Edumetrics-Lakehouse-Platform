"""
============================================================================
FILE: silver_gold_pipeline.py
============================================================================

OBJECTIVE:
    Build Silver and Gold layers of the Medallion Architecture.
    Silver = cleaned and enriched data.
    Gold   = aggregated business metrics ready for dashboards.

DATA FLOW:
    delta/bronze_funnel_logs
        → [Clean + Enrich]  → delta/silver_funnel_events
        → [Aggregate + SQL] → delta/gold_daily_funnel_metrics

KEY CONCEPTS:
    - Silver Layer: deduplication, null handling, type casting, enrichment
    - Gold Layer: business aggregations using Spark SQL + Window Functions
    - Partitioning by event_date for efficient date-range queries

INTERVIEW QUESTIONS:
    1. What is the difference between Silver and Gold layers?
       → Silver = cleaned individual records. Gold = aggregated metrics.
    2. Why partition by event_date?
       → Date-range queries only scan relevant partitions (partition pruning).
    3. What are Window Functions in Spark SQL?
       → Functions that compute values across a "window" of related rows
         (e.g., running totals, rankings) without collapsing rows.
    4. What is Z-Ordering?
       → A Delta Lake optimization that co-locates related data on disk
         to minimize file scanning for filtered queries.
============================================================================
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

# ──────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────

BRONZE_TABLE = "delta/bronze_funnel_logs"
SILVER_TABLE = "delta/silver_funnel_events"
GOLD_TABLE = "delta/gold_daily_funnel_metrics"


def get_spark():
    from delta import configure_spark_with_delta_pip

    # Set HADOOP_HOME for Windows (needed for winutils.exe)
    hadoop_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hadoop")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

    builder = (
        SparkSession.builder
        .appName("EduMetrics_SilverGold")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ──────────────────────────────────────────────────────────────────────────
# SILVER LAYER — Clean, Enrich, Flatten
# ──────────────────────────────────────────────────────────────────────────

def build_silver(spark):
    """
    Transform Bronze → Silver.
    Steps: parse timestamps, extract course from URL, drop duplicates.
    """
    print("\n🥈 Silver Layer — Cleaning & Enrichment")
    print("-" * 50)

    if not os.path.exists(BRONZE_TABLE):
        print("  ❌ Bronze table not found. Run bronze_pipeline.py first.")
        return 0

    df = spark.read.format("delta").load(BRONZE_TABLE)
    print(f"  📥 Read {df.count():,} rows from Bronze")

    # 1. Parse timestamp string → proper timestamp type + extract date
    df = df.withColumn("event_ts", F.to_timestamp("timestamp"))
    df = df.withColumn("event_date", F.to_date("event_ts"))

    # 2. Extract course slug from page_url (e.g., '/courses/dsa-self-paced' → 'dsa-self-paced')
    df = df.withColumn(
        "page_course_slug",
        F.when(F.col("page_url").startswith("/courses/"),
               F.regexp_extract("page_url", r"^/courses/(.+)$", 1))
    )

    # 3. Categorize pages (flatten URL structure into a category)
    df = df.withColumn("url_category",
        F.when(F.col("page_url").startswith("/courses/"), "course_page")
         .when(F.col("page_url").startswith("/practice/"), "practice_page")
         .when(F.col("page_url").startswith("/articles/"), "article_page")
         .otherwise("general_page")
    )

    # 4. Create a unified course identifier across event types
    df = df.withColumn("course", F.coalesce("course_name", "page_course_slug"))

    # 5. Add processing timestamp for audit trail
    df = df.withColumn("processed_at", F.current_timestamp())

    # 6. Drop duplicate rows (deduplication)
    before = df.count()
    df = df.dropDuplicates(["event_id", "checkout_id", "order_id"])
    after = df.count()
    print(f"  🧹 Deduplication: {before:,} → {after:,} ({before - after} removed)")

    # 7. Write Silver table, partitioned by event_date
    df.write.format("delta").mode("overwrite").partitionBy("event_date").save(SILVER_TABLE)
    print(f"  ✅ Silver table written: {after:,} rows → {SILVER_TABLE}")

    return after


# ──────────────────────────────────────────────────────────────────────────
# GOLD LAYER — Aggregated Business Metrics using Spark SQL
# ──────────────────────────────────────────────────────────────────────────

def build_gold(spark):
    """
    Transform Silver → Gold.
    Aggregate funnel metrics per (date, course) using Spark SQL and Window Functions.
    """
    print("\n🥇 Gold Layer — Funnel Metrics Aggregation")
    print("-" * 50)

    if not os.path.exists(SILVER_TABLE):
        print("  ❌ Silver table not found. Build Silver first.")
        return 0

    silver = spark.read.format("delta").load(SILVER_TABLE)
    silver.createOrReplaceTempView("silver")

    # ── SPARK SQL: Aggregate metrics per date and course ──
    # This is the core analytics query that powers the dashboard
    gold = spark.sql("""
        WITH page_views AS (
            -- Count page views and unique sessions from clickstream
            SELECT event_date, course,
                   COUNT(*)                    AS total_page_views,
                   COUNT(DISTINCT session_id)  AS unique_sessions
            FROM silver
            WHERE event_type = 'clickstream' AND event_date IS NOT NULL
            GROUP BY event_date, course
        ),
        checkouts AS (
            -- Count checkout funnel stages
            SELECT event_date, course,
                   COUNT(*) AS checkout_initiations,
                   SUM(CASE WHEN step_reached = 'payment_gateway_redirect'
                            THEN 1 ELSE 0 END) AS gateway_reaches,
                   SUM(CASE WHEN step_reached = 'payment_gateway_redirect'
                            THEN price_inr ELSE 0 END) AS potential_revenue
            FROM silver
            WHERE event_type = 'checkout' AND event_date IS NOT NULL
            GROUP BY event_date, course
        ),
        orders AS (
            -- Count completed purchases
            SELECT event_date, course,
                   COUNT(*)        AS completed_purchases,
                   SUM(amount_paid) AS total_revenue
            FROM silver
            WHERE event_type = 'order' AND payment_status = 'SUCCESS'
                  AND event_date IS NOT NULL
            GROUP BY event_date, course
        )
        -- Full outer join: some dates may have views but no orders, etc.
        SELECT
            COALESCE(p.event_date, c.event_date, o.event_date) AS event_date,
            COALESCE(p.course, c.course, o.course)             AS course_name,
            COALESCE(total_page_views, 0)      AS total_page_views,
            COALESCE(unique_sessions, 0)       AS unique_sessions,
            COALESCE(checkout_initiations, 0)  AS checkout_initiations,
            COALESCE(gateway_reaches, 0)       AS gateway_reaches,
            COALESCE(completed_purchases, 0)   AS completed_purchases,
            COALESCE(total_revenue, 0)         AS total_revenue,
            COALESCE(potential_revenue, 0)      AS potential_revenue,
            -- Conversion Rate = purchases / checkouts × 100
            ROUND(COALESCE(completed_purchases, 0) * 100.0
                  / NULLIF(COALESCE(checkout_initiations, 0), 0), 2) AS conversion_rate_pct,
            -- Lost Revenue = potential revenue at gateway - actual revenue
            COALESCE(potential_revenue, 0) - COALESCE(total_revenue, 0) AS lost_revenue
        FROM page_views p
        FULL OUTER JOIN checkouts c ON p.event_date = c.event_date AND p.course = c.course
        FULL OUTER JOIN orders o    ON COALESCE(p.event_date, c.event_date) = o.event_date
                                   AND COALESCE(p.course, c.course) = o.course
    """)

    # ── WINDOW FUNCTIONS: Add running totals and rankings ──
    date_window = Window.partitionBy("course_name").orderBy("event_date")
    rank_window = Window.partitionBy("event_date").orderBy(F.desc("total_revenue"))

    gold = gold.withColumn("cumulative_revenue", F.sum("total_revenue").over(date_window))
    gold = gold.withColumn("revenue_rank", F.dense_rank().over(rank_window))

    # Filter out rows where event_date is null
    gold = gold.filter(F.col("event_date").isNotNull())

    # Write Gold table
    count = gold.count()
    gold.write.format("delta").mode("overwrite").save(GOLD_TABLE)
    print(f"  ✅ Gold table written: {count:,} rows → {GOLD_TABLE}")

    # Show preview
    print("\n  📊 Sample Gold Metrics:")
    gold.orderBy("event_date", "course_name").show(10, truncate=False)
    return count


# ──────────────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────────────────

def run_pipeline():
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("=" * 60)
    print("⚙️  Silver → Gold Transformation Pipeline")
    print("=" * 60)

    spark = get_spark()
    silver_rows = build_silver(spark)
    gold_rows = build_gold(spark)

    print(f"\n📈 Pipeline Complete: Silver={silver_rows:,}  Gold={gold_rows:,}")
    spark.stop()

if __name__ == "__main__":
    run_pipeline()

"""
SAMPLE OUTPUT:
============================================================
⚙️  Silver → Gold Transformation Pipeline
============================================================

🥈 Silver Layer — Cleaning & Enrichment
--------------------------------------------------
  📥 Read 1,246 rows from Bronze
  🧹 Deduplication: 1,246 → 1,246 (0 removed)
  ✅ Silver table written: 1,246 rows

🥇 Gold Layer — Funnel Metrics Aggregation
--------------------------------------------------
  ✅ Gold table written: 75 rows

  📊 Sample Gold Metrics:
  +----------+------------------+----------+--------+----------+--------+----------+------+---------+
  |event_date|course_name       |page_views|sessions|checkouts |gateway |purchases |rev   |lost_rev |
  +----------+------------------+----------+--------+----------+--------+----------+------+---------+
  |2026-06-01|DSA Self-Paced    |45        |32      |8         |3       |2         |4998  |2499     |
  |2026-06-01|Interview Prep    |38        |28      |6         |2       |1         |3999  |3999     |
  +----------+------------------+----------+--------+----------+--------+----------+------+---------+
"""
