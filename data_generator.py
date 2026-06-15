"""
============================================================================
FILE: data_generator.py
============================================================================

OBJECTIVE:
    Simulate e-learning platform user activity data (like GeeksforGeeks).
    Generate 3 datasets: clickstream, checkouts, and orders.

DATA FLOW:
    Script → clickstream_events.json
           → checkout_attempts.json
           → successful_orders.csv

KEY CONCEPTS:
    - Synthetic data generation for testing pipelines
    - Hive-style partitioning (year/month/day folders)
    - Deliberate bad data injection (5%) to test quality checks

INTERVIEW QUESTIONS:
    1. Why do we inject bad records deliberately?
       → To test our downstream data quality checks and quarantine logic.
    2. What is Hive-style partitioning?
       → Organizing files into year=/month=/day= folders for efficient reads.
    3. Why use UUID for IDs?
       → Ensures globally unique identifiers without a central database.
============================================================================
"""

import csv
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# ──────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────

TOTAL_RECORDS = 1000          # Total clickstream events to generate
BAD_RECORD_RATE = 0.05        # 5% records will have missing/null fields
NUM_USERS = 200               # Size of the synthetic user pool

COURSES = [
    {"id": "CRS-001", "name": "Complete Interview Preparation", "price": 3999},
    {"id": "CRS-002", "name": "DSA Self-Paced",                 "price": 2499},
    {"id": "CRS-003", "name": "System Design Live",             "price": 7999},
    {"id": "CRS-004", "name": "Full Stack Development",         "price": 9999},
    {"id": "CRS-005", "name": "Machine Learning Foundation",    "price": 5999},
]

PAGE_URLS = [
    "/courses/interview-prep", "/courses/dsa-self-paced",
    "/courses/system-design",  "/courses/full-stack",
    "/courses/machine-learning", "/practice/problems",
    "/articles/tutorials", "/home",
]

CHECKOUT_STEPS = ["cart_view", "billing_details", "payment_gateway_redirect"]

# Output directories (Hive-style partitioning)
TODAY = datetime(2026, 6, 15)
OUTPUT_DIR = Path(f"data/raw/year={TODAY.year}/month={TODAY.month:02d}/day={TODAY.day:02d}")


# ──────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ──────────────────────────────────────────────────────────────────────────

def random_timestamp():
    """Generate a random timestamp within the last 15 days."""
    offset = timedelta(days=random.randint(0, 14), hours=random.randint(0, 23),
                       minutes=random.randint(0, 59))
    return (TODAY - offset).isoformat()

def random_user_id():
    return f"USR-{random.randint(1, NUM_USERS):05d}"

def should_corrupt(index):
    """Every 20th record (5%) gets corrupted with null/empty fields."""
    return index % 20 == 0


# ──────────────────────────────────────────────────────────────────────────
# GENERATOR 1: CLICKSTREAM EVENTS (page views)
# ──────────────────────────────────────────────────────────────────────────

def generate_clickstream(count):
    """
    Simulates user browsing activity.
    Each record = one page view on the platform.
    Bad records: session_id or timestamp set to null/empty.
    """
    events = []
    for i in range(count):
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random_user_id(),
            "session_id": "" if should_corrupt(i) else f"SES-{uuid.uuid4().hex[:10]}",
            "timestamp": None if should_corrupt(i) else random_timestamp(),
            "page_url": random.choice(PAGE_URLS),
            "action": "view_page",
        }
        events.append(event)
    return events


# ──────────────────────────────────────────────────────────────────────────
# GENERATOR 2: CHECKOUT ATTEMPTS (funnel tracking)
# ──────────────────────────────────────────────────────────────────────────

def generate_checkouts(count):
    """
    Simulates checkout funnel stages.
    step_reached shows how far the user got:
      cart_view → billing_details → payment_gateway_redirect
    """
    attempts = []
    for i in range(count):
        course = random.choice(COURSES)
        attempt = {
            "checkout_id": str(uuid.uuid4()),
            "user_id": random_user_id(),
            "session_id": None if should_corrupt(i) else f"SES-{uuid.uuid4().hex[:10]}",
            "course_id": course["id"],
            "course_name": course["name"],
            "price_inr": course["price"],
            "step_reached": random.choice(CHECKOUT_STEPS),
            "timestamp": "" if should_corrupt(i) else random_timestamp(),
        }
        attempts.append(attempt)
    return attempts


# ──────────────────────────────────────────────────────────────────────────
# GENERATOR 3: SUCCESSFUL ORDERS (completed purchases)
# ──────────────────────────────────────────────────────────────────────────

def generate_orders(checkouts):
    """
    Only users who reached 'payment_gateway_redirect' CAN purchase.
    30% of those users abandon (Ghost Shoppers) → they never appear here.
    This simulates real cart abandonment behavior.
    """
    orders = []
    for co in checkouts:
        if co["step_reached"] != "payment_gateway_redirect":
            continue  # Only gateway users can convert
        if random.random() < 0.30:
            continue  # 30% ghost shoppers → deliberately skip
        orders.append({
            "order_id": str(uuid.uuid4()),
            "checkout_id": co["checkout_id"],
            "user_id": co["user_id"],
            "amount_paid": co["price_inr"],
            "payment_status": "SUCCESS",
            "timestamp": random_timestamp(),
        })
    return orders


# ──────────────────────────────────────────────────────────────────────────
# FILE WRITERS
# ──────────────────────────────────────────────────────────────────────────

def save_json(data, filename):
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  ✅ Saved {len(data):,} records → {filepath}")

def save_csv(data, filename):
    filepath = OUTPUT_DIR / filename
    if not data:
        print(f"  ⚠️  No records to save → {filename}")
        return
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  ✅ Saved {len(data):,} records → {filepath}")


# ──────────────────────────────────────────────────────────────────────────
# MAIN EXECUTION
# ──────────────────────────────────────────────────────────────────────────

def main():
    # Fix Unicode output on Windows terminals
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print("=" * 60)
    print("📊 EduMetrics Data Generator")
    print("=" * 60)

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate all datasets
    clicks = generate_clickstream(TOTAL_RECORDS)
    checkouts = generate_checkouts(TOTAL_RECORDS // 4)   # 250 checkout attempts
    orders = generate_orders(checkouts)

    # Save to files
    save_json(clicks, "clickstream_events.json")
    save_json(checkouts, "checkout_attempts.json")
    save_csv(orders, "successful_orders.csv")

    # Summary
    bad_clicks = sum(1 for e in clicks if not e["session_id"] or e["timestamp"] is None)
    gateway = sum(1 for c in checkouts if c["step_reached"] == "payment_gateway_redirect")
    ghosts = gateway - len(orders)

    print(f"\n📈 Summary:")
    print(f"  Clickstream events : {len(clicks):,}")
    print(f"  Checkout attempts  : {len(checkouts):,}")
    print(f"  Successful orders  : {len(orders):,}")
    print(f"  Bad records        : {bad_clicks} ({bad_clicks/len(clicks)*100:.1f}%)")
    print(f"  Ghost shoppers     : {ghosts} of {gateway} gateway users")

if __name__ == "__main__":
    main()

"""
SAMPLE OUTPUT:
============================================================
📊 EduMetrics Data Generator
============================================================
  ✅ Saved 1,000 records → data/raw/year=2026/month=06/day=15/clickstream_events.json
  ✅ Saved 250 records   → data/raw/year=2026/month=06/day=15/checkout_attempts.json
  ✅ Saved 58 records    → data/raw/year=2026/month=06/day=15/successful_orders.csv

📈 Summary:
  Clickstream events : 1,000
  Checkout attempts  : 250
  Successful orders  : 58
  Bad records        : 50 (5.0%)
  Ghost shoppers     : 25 of 83 gateway users
"""
