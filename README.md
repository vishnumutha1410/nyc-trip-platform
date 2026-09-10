# 🔥 Wildfire Risk & Air Quality Monitoring Pipeline

An end-to-end automated data engineering pipeline that monitors wildfire-risk conditions and air quality across major US regions every hour — built with Python, Snowflake, dbt, Apache Airflow, and Docker.

---

## 📌 What It Does

Every hour this pipeline automatically:
1. **Extracts** live weather (temperature, humidity, wind) and air-quality (PM2.5, PM10) data from the Open-Meteo public API for 6 regions
2. **Loads** the raw readings into a Snowflake cloud data warehouse
3. **Transforms** the raw data using dbt into clean analytics tables with a fire-risk score and air-quality classification per region
4. **Tests** the output data automatically with dbt data-quality checks
5. **Orchestrates** the full sequence with Apache Airflow on an hourly schedule
6. **Containerizes** everything with Docker for reproducibility

---

## 🏗️ Architecture

```
Open-Meteo API
     │
     ▼
Python Ingestion Script (ingest.py)
     │  extracts weather + air quality as JSON
     ▼
Snowflake — RAW Schema (AIR_WEATHER_RAW)
     │  raw readings, timestamped every hour
     ▼
dbt — Transformation Layer
     ├── stg_air_weather      (clean + type-cast)
     ├── fct_fire_risk        (fire-risk score 0-100 + category)
     └── fct_air_quality      (PM2.5 health bands + unhealthy flag)
     │
     ▼
Snowflake — ANALYTICS Schema
     │  clean, tested, query-ready tables
     ▼
Apache Airflow (orchestration, @hourly schedule)
     │
Docker (containerization)
```

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Language | Python | Ingestion script, API calls |
| Data Source | Open-Meteo API | Free live weather + air quality data |
| Warehouse | Snowflake | Cloud data storage (RAW + ANALYTICS) |
| Transform | dbt | SQL models, data-quality tests |
| Orchestration | Apache Airflow | Hourly scheduling, DAG management |
| Containerization | Docker | Reproducible environment |
| Version Control | Git / GitHub | Code hosting |

---

## 📊 Sample Output

Fire-risk scores calculated from real live data:

| Region | Fire Risk Level | Fire Risk Score | Temperature |
|---|---|---|---|
| Denver | **Extreme** | 79.7 | 33.6°C |
| Phoenix | **Extreme** | 79.3 | 43.1°C |
| Los Angeles | **High** | 64.7 | 37.1°C |
| Sacramento | **Moderate** | 52.3 | 28.4°C |
| San Francisco | **Low** | 31.2 | 17.4°C |
| Portland | **Moderate** | 44.1 | 24.4°C |

> 42+ raw readings collected and counting — pipeline runs every hour automatically.

---

## 🔬 dbt Models

### Staging
- **`stg_air_weather`** — Cleans and type-casts raw API readings; filters null records

### Marts
- **`fct_fire_risk`** — Calculates a 0-100 fire-risk score from temperature (40%), humidity (35%), and wind (25%); buckets into Low / Moderate / High / Extreme
- **`fct_air_quality`** — Classifies PM2.5 into WHO/EPA health bands (Good → Hazardous); flags unhealthy readings

### Data Quality Tests
- `not_null` checks on all key columns
- `accepted_values` checks on `fire_risk_level` and `air_quality_category`
- All 8 tests passing ✅

---

## ⚙️ Airflow DAG

DAG ID: `wildfire_risk_pipeline`
Schedule: `@hourly`
Tasks (in order):

```
ingest_weather_and_air_quality >> dbt_run >> dbt_test
```

- **Task 1** — runs `ingest.py` to pull from API and load to Snowflake
- **Task 2** — runs `dbt run` to build clean analytics tables
- **Task 3** — runs `dbt test` to validate data quality

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop installed and running
- Snowflake account (free trial works)
- Python 3.9+

### 1. Clone the repo
```bash
git clone https://github.com/vishnumutha1410/wildfire-risk-pipeline.git
cd wildfire-risk-pipeline
```

### 2. Set up your Snowflake database
Run this SQL in your Snowflake worksheet:
```sql
CREATE DATABASE IF NOT EXISTS WILDFIRE_DB;
CREATE SCHEMA IF NOT EXISTS WILDFIRE_DB.RAW;
CREATE SCHEMA IF NOT EXISTS WILDFIRE_DB.ANALYTICS;
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### 3. Configure credentials
Copy `ingestion/.env.example` to `ingestion/.env` and fill in your Snowflake details:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=WILDFIRE_DB
```

### 4. Launch with Docker
```bash
docker compose up
```

### 5. Open Airflow
Go to `http://localhost:8080` (admin / admin), enable the `wildfire_risk_pipeline` DAG, and trigger a run.

---

## 📁 Project Structure

```
wildfire-risk-pipeline/
├── ingestion/
│   ├── ingest.py              # Python ingestion script
│   ├── requirements.txt       # Python dependencies
│   └── .env.example           # Credentials template
├── wildfire_dbt/
│   ├── dbt_project.yml        # dbt project config
│   └── models/
│       ├── staging/
│       │   ├── _sources.yml
│       │   └── stg_air_weather.sql
│       └── marts/
│           ├── fct_fire_risk.sql
│           ├── fct_air_quality.sql
│           └── _schema.yml
├── dags/
│   └── wildfire_pipeline_dag.py   # Airflow DAG
├── docker-compose.yaml            # Docker setup
├── .gitignore
└── README.md
```

---

## 🔒 Security

- Credentials stored in `.env` file, never hard-coded
- `.env` and `docker_dbt_profile/` excluded from Git via `.gitignore`

---

## 👤 Author

**Vishnu Vardhan Mutha**
Data Engineer & Data Analyst | MS in Computer Science (Data Analytics) — Webster University

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/vishnu-vardhan-mutha-5187942b9)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/vishnumutha1410)
[![Portfolio](https://img.shields.io/badge/Portfolio-22D3A7?style=flat&logo=googlechrome&logoColor=white)](https://vishnumutha1410.github.io/portfolio)
