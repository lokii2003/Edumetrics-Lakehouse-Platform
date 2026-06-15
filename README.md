# Enterprise E-Learning Lakehouse Analytics Platform

A production-ready Data Engineering portfolio project showcasing **PySpark**, **Delta Lake**, **Medallion Architecture**, **Spark SQL**, **Airflow**, and **Streamlit**.

> Built for Data Engineering interviews. Clean code. Clear explanations. Easy to run.

---

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌──────────────┐
│ data_generator   │───▶│ bronze_pipeline  │───▶│ silver_gold_    │───▶│ app.py       │
│                  │    │                  │    │ pipeline        │    │ (Streamlit)  │
│ Clickstream JSON │    │ Schema Validate  │    │                 │    │              │
│ Checkout JSON    │    │ Quarantine Bad   │    │ Silver: Clean   │    │ KPI Cards    │
│ Orders CSV       │    │ Bronze Delta ✅   │    │ Gold: Metrics   │    │ Funnel Chart │
│ 5% Bad Records   │    │ Quarantine ❌     │    │ Spark SQL       │    │ Revenue Pie  │
└─────────────────┘    └─────────────────┘    │ Window Funcs    │    │ Trend Line   │
                                               └─────────────────┘    │ CSV Export   │
                                                                      └──────────────┘
                       Orchestrated by: dags/funnel_dag.py (Airflow)
```

---

## 📁 Folder Structure

```
elearning-lakehouse/
├── data_generator.py          # Generates 1000+ simulated records
├── pipeline_bronze.py         # Bronze layer: ingestion + validation
├── pipeline_silver_gold.py    # Silver + Gold layers: cleaning + aggregation
├── app.py                     # Streamlit dashboard with Plotly charts
├── dags/
│   └── funnel_dag.py          # Airflow DAG for daily orchestration
├── data/raw/                  # Generated raw data (Hive-partitioned)
├── delta/                     # Delta Lake tables (auto-created)
├── requirements.txt
├── .gitignore
└── README.md
```

---
## Dashboard Overview

![Dashboard](Screenshots/Screenshot 2026-06-15 215617.png)
![Dashboard](Screenshots/Screenshot 2026-06-15 215609.png)



## 🛠️ Technologies Used

| Technology | Purpose |
|-----------|---------|
| **PySpark** | Distributed data processing |
| **Delta Lake** | ACID transactions, time travel |
| **Spark SQL** | Advanced SQL aggregations |
| **Streamlit** | Interactive dashboard |
| **Plotly** | Rich chart visualizations |
| **Airflow** | Pipeline orchestration |
| **Python** | Core scripting language |

---

## 📊 Data Flow (Medallion Architecture)

| Layer | Table | Description |
|-------|-------|-------------|
| **Raw** | `data/raw/` | JSON + CSV files with 5% bad records |
| **Bronze** | `bronze_funnel_logs` | Validated raw data (clean rows) |
| **Quarantine** | `quarantine_invalid_logs` | Rejected rows (null session/timestamp) |
| **Silver** | `silver_funnel_events` | Cleaned, enriched, deduplicated |
| **Gold** | `gold_daily_funnel_metrics` | Aggregated funnel metrics per date/course |

---

## 🚀 How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate data
python data_generator.py

# 3. Run Bronze pipeline
python pipeline_bronze.py

# 4. Run Silver + Gold pipeline
python pipeline_silver_gold.py

# 5. Launch dashboard
streamlit run app.py
```

> **Tip:** The dashboard works immediately with demo data — no pipeline needed!

---

## 📸 Screenshots

_Add screenshots of your Streamlit dashboard here after running the app._

---

## 🎯 Interview Questions Covered

- What is Medallion Architecture?
- Explain Bronze, Silver, Gold layers.
- How does Delta Lake differ from Parquet?
- What are Window Functions in Spark SQL?
- How do you handle data quality in pipelines?
- What is schema-on-read?
- Why partition by date?
- How does Airflow orchestrate tasks?
- What is cart abandonment analysis?
- How do you calculate conversion rate?

---

## 🔮 Future Improvements

- Add Databricks Auto Loader for streaming ingestion
- Implement Unity Catalog for data governance
- Add dbt for SQL transformations
- Deploy dashboard on Streamlit Cloud
- Add Great Expectations for data quality
- Implement CI/CD with GitHub Actions

---

## 📝 License

MIT — Built for educational and portfolio purposes.
