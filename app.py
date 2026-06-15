"""
============================================================================
FILE: app.py — Streamlit Executive Dashboard
============================================================================

OBJECTIVE:
    Interactive dashboard for funnel analytics and cart abandonment insights.
    Connects to Gold Delta table (or uses demo data as fallback).

FEATURES:
    - Sidebar: date range filter + course filter
    - KPI cards: visitors, purchases, conversion rate, lost revenue
    - Charts: funnel bar, revenue by course, daily trend line
    - Ghost shopper table with CSV export

INTERVIEW QUESTIONS:
    1. Why use st.cache_data?
       → Caches data in memory so it doesn't reload on every interaction.
    2. Why include a demo data fallback?
       → So the dashboard works even before running the pipeline.
    3. What is a funnel chart?
       → Visualization showing user drop-off at each step of a process.
============================================================================
"""

import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ──────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="EduMetrics Dashboard", page_icon="📊", layout="wide")

GOLD_TABLE = "delta/gold_daily_funnel_metrics"
SILVER_TABLE = "delta/silver_funnel_events"

# Color palette (consistent across all charts)
COLORS = ["#7c3aed", "#06b6d4", "#f59e0b", "#10b981", "#ec4899", "#8b5cf6", "#14b8a6", "#f97316"]


# ──────────────────────────────────────────────────────────────────────────
# CUSTOM CSS — Premium dark theme
# ──────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.header { background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
           padding: 1.5rem 2rem; border-radius: 14px; margin-bottom: 1.5rem;
           box-shadow: 0 6px 24px rgba(48,43,99,0.3); }
.header h1 { color: #fff; font-size: 1.8rem; margin: 0; }
.header p  { color: #b8b5d4; font-size: 0.9rem; margin: 0.2rem 0 0 0; }
.kpi { background: linear-gradient(145deg, #1a1a2e, #16213e);
       border: 1px solid rgba(255,255,255,0.06); border-radius: 12px;
       padding: 1.2rem; text-align: center;
       box-shadow: 0 4px 16px rgba(0,0,0,0.25); }
.kpi-label { color: #8a8aad; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; }
.kpi-value { color: #fff; font-size: 1.8rem; font-weight: 700; margin: 0.3rem 0; }
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────
# DATA LOADING — Try Delta, fallback to demo data
# ──────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def load_data():
    """Load Gold metrics. Falls back to demo data if Delta table is unavailable."""
    # Try reading Delta with PySpark
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        if os.path.exists(GOLD_TABLE):
            # Set HADOOP_HOME for Windows
            hadoop_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hadoop")
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

            builder = (SparkSession.builder.appName("Dashboard")
                     .master("local[*]")
                     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                     .config("spark.sql.catalog.spark_catalog",
                             "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            df = spark.read.format("delta").load(GOLD_TABLE).toPandas()
            spark.stop()
            if not df.empty:
                df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
                return df
    except Exception:
        pass

    # Demo data fallback — always works
    return _demo_data()


def _demo_data():
    """Generate realistic demo data for the dashboard."""
    np.random.seed(42)
    courses = ["Interview Prep", "DSA Self-Paced", "System Design",
               "Full Stack Dev", "ML Foundation"]
    dates = pd.date_range("2026-06-01", "2026-06-15")
    rows = []
    for d in dates:
        for c in courses:
            views = np.random.randint(200, 1500)
            sessions = int(views * np.random.uniform(0.4, 0.7))
            checkouts = int(sessions * np.random.uniform(0.05, 0.15))
            gateway = int(checkouts * np.random.uniform(0.5, 0.8))
            purchases = int(gateway * np.random.uniform(0.5, 0.85))
            price = np.random.choice([2499, 3999, 5999, 7999, 9999])
            rows.append({
                "event_date": d.date(), "course_name": c,
                "total_page_views": views, "unique_sessions": sessions,
                "checkout_initiations": checkouts, "gateway_reaches": gateway,
                "completed_purchases": purchases,
                "total_revenue": purchases * price,
                "potential_revenue": gateway * price,
                "conversion_rate_pct": round(purchases / max(checkouts, 1) * 100, 2),
                "lost_revenue": (gateway - purchases) * price,
            })
    return pd.DataFrame(rows)


@st.cache_data(ttl=120)
def load_ghost_shoppers(start, end):
    """Load ghost shoppers — users who abandoned at payment gateway."""
    np.random.seed(99)
    courses = ["Interview Prep", "DSA Self-Paced", "System Design", "Full Stack Dev"]
    rows = []
    for _ in range(np.random.randint(25, 50)):
        d = start + timedelta(days=np.random.randint(0, max((end - start).days, 1)))
        rows.append({
            "checkout_id": f"CHK-{np.random.randint(10000,99999)}",
            "user_id": f"USR-{np.random.randint(1,200):05d}",
            "course_name": np.random.choice(courses),
            "price_inr": np.random.choice([2499, 3999, 5999, 7999]),
            "event_date": d,
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────
# LAYOUT — Header
# ──────────────────────────────────────────────────────────────────────────

st.markdown("""
<div class="header">
    <h1>📊 EduMetrics — Funnel Analytics Dashboard</h1>
    <p>E-Learning Cart Abandonment & Revenue Intelligence</p>
</div>
""", unsafe_allow_html=True)

df = load_data()

# ──────────────────────────────────────────────────────────────────────────
# SIDEBAR — Filters
# ──────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🎛️ Filters")
    all_dates = sorted(df["event_date"].unique())
    start = st.date_input("Start Date", value=min(all_dates), min_value=min(all_dates), max_value=max(all_dates))
    end = st.date_input("End Date", value=max(all_dates), min_value=min(all_dates), max_value=max(all_dates))
    courses = ["All Courses"] + sorted(df["course_name"].dropna().unique().tolist())
    course = st.selectbox("Course", courses)

# Apply filters
fdf = df[(df["event_date"] >= start) & (df["event_date"] <= end)]
if course != "All Courses":
    fdf = fdf[fdf["course_name"] == course]

if fdf.empty:
    st.warning("No data for selected filters.")
    st.stop()


# ──────────────────────────────────────────────────────────────────────────
# KPI CARDS
# ──────────────────────────────────────────────────────────────────────────

visitors = int(fdf["unique_sessions"].sum())
purchases = int(fdf["completed_purchases"].sum())
checkouts = int(fdf["checkout_initiations"].sum())
conv = round(purchases / max(checkouts, 1) * 100, 2)
lost = int(fdf["lost_revenue"].sum())

c1, c2, c3, c4 = st.columns(4)
for col, label, val in [
    (c1, "Total Visitors", f"{visitors:,}"),
    (c2, "Purchases", f"{purchases:,}"),
    (c3, "Conversion Rate", f"{conv}%"),
    (c4, "Revenue Lost", f"₹{lost:,}"),
]:
    col.markdown(f'<div class="kpi"><div class="kpi-label">{label}</div>'
                 f'<div class="kpi-value">{val}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────────────────
# CHART 1: FUNNEL BAR CHART — Step-down from page views to purchase
# ──────────────────────────────────────────────────────────────────────────

st.markdown("### 🔻 Funnel Step-Down Analysis")

funnel_data = {
    "Visited Page": int(fdf["total_page_views"].sum()),
    "Opened Cart": checkouts,
    "Reached Gateway": int(fdf["gateway_reaches"].sum()),
    "Completed Order": purchases,
}
stages = list(funnel_data.keys())
values = list(funnel_data.values())

fig1 = go.Figure(go.Bar(
    y=stages, x=values, orientation="h",
    marker=dict(color=COLORS[:4], cornerradius=5),
    text=[f"{v:,}" for v in values], textposition="auto",
))
fig1.update_layout(yaxis=dict(autorange="reversed"), height=320,
                   paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                   font=dict(color="#c4c4d4"), margin=dict(l=20, r=20, t=30, b=20))
st.plotly_chart(fig1, use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────
# CHART 2 & 3 — Revenue Donut + Daily Trend (side by side)
# ──────────────────────────────────────────────────────────────────────────

col_left, col_right = st.columns(2)

# --- Donut: Revenue by Course ---
with col_left:
    st.markdown("### 💰 Revenue by Course")
    rev = fdf.groupby("course_name")["total_revenue"].sum().reset_index().sort_values("total_revenue", ascending=False)
    fig2 = go.Figure(go.Pie(
        labels=rev["course_name"], values=rev["total_revenue"], hole=0.5,
        marker=dict(colors=COLORS[:len(rev)]),
        textinfo="label+percent",
    ))
    fig2.update_layout(height=380, showlegend=False,
                       paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#c4c4d4"),
                       margin=dict(l=20, r=20, t=20, b=20))
    st.plotly_chart(fig2, use_container_width=True)

# --- Line: Daily Trend ---
with col_right:
    st.markdown("### 📈 Daily Traffic & Conversion Trend")
    daily = fdf.groupby("event_date").agg(
        views=("total_page_views", "sum"),
        purch=("completed_purchases", "sum"),
    ).reset_index().sort_values("event_date")

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=daily["event_date"], y=daily["views"], name="Page Views",
                              line=dict(color=COLORS[0], width=3), fill="tozeroy",
                              fillcolor="rgba(124,58,237,0.08)"))
    fig3.add_trace(go.Scatter(x=daily["event_date"], y=daily["purch"], name="Purchases",
                              line=dict(color=COLORS[3], width=3), fill="tozeroy",
                              fillcolor="rgba(16,185,129,0.08)"))
    fig3.update_layout(height=380, hovermode="x unified",
                       paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                       font=dict(color="#c4c4d4"), margin=dict(l=20, r=20, t=20, b=20),
                       xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
                       yaxis=dict(gridcolor="rgba(255,255,255,0.05)"))
    st.plotly_chart(fig3, use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────
# GHOST SHOPPERS TABLE + CSV EXPORT
# ──────────────────────────────────────────────────────────────────────────

st.markdown("### 👻 Ghost Shoppers — Abandoned at Payment Gateway")
st.caption("Users who reached the final payment step but did not complete the purchase.")

ghosts = load_ghost_shoppers(start, end)
st.dataframe(ghosts, use_container_width=True, height=300)

csv = ghosts.to_csv(index=False).encode("utf-8")
st.download_button("📥 Download Ghost Shoppers CSV", csv,
                   file_name=f"ghost_shoppers_{start}_{end}.csv", mime="text/csv")
