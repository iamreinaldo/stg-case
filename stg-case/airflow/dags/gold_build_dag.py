import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import snowflake.connector

# -----------------------------------------------------------------------------
# Configs (override via env vars if needed)
# -----------------------------------------------------------------------------
DB_NAME = os.getenv("COVID_DB_NAME", "COVID_DB")
BRONZE_SCHEMA = os.getenv("COVID_BRONZE_SCHEMA", "BRONZE")
SILVER_SCHEMA = os.getenv("COVID_SILVER_SCHEMA", "SILVER")
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")

# Fully-qualified helpers
B = f"{DB_NAME}.{BRONZE_SCHEMA}"
S = f"{DB_NAME}.{SILVER_SCHEMA}"

# Snowflake connection config from env
SF_CFG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": DB_NAME,
    "schema": BRONZE_SCHEMA,
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

def run_sql(sql: str):
    ctx = snowflake.connector.connect(**SF_CFG)
    cs = ctx.cursor()
    try:
        for stmt in sql.strip().split(";"):
            if stmt.strip():
                cs.execute(stmt)
    finally:
        cs.close()
        ctx.close()

# -----------------------------------------------------------------------------
# GOLD configs/helpers
# -----------------------------------------------------------------------------
GOLD_SCHEMA = os.getenv("COVID_GOLD_SCHEMA", "GOLD")
G = f"{DB_NAME}.{GOLD_SCHEMA}"

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="silver_to_gold",
    description="Constrói a camada GOLD (tabela fato e views) a partir do SILVER",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["covid", "gold", "snowflake"],
):
    create_gold_schema = PythonOperator(
        task_id="create_gold_schema",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA};
                USE SCHEMA {GOLD_SCHEMA};
            """,
        },
    )

    build_fact_covid_daily = PythonOperator(
        task_id="build_fact_covid_daily",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {GOLD_SCHEMA};

                CREATE OR REPLACE TABLE {G}.FACT_COVID_DAILY AS
                WITH base AS (
                  SELECT
                    c.ISO_CODE,
                    c.DATE,
                    c.COUNTRY,
                    c.CONTINENT,
                    d.POPULATION,

                    /* Cases */
                    c.NEW_CASES,
                    c.TOTAL_CASES,
                    c.NEW_DEATHS,
                    c.TOTAL_DEATHS,

                    /* Tests */
                    t.NEW_TESTS,
                    t.NEW_TESTS_PER_THOUSAND,
                    t.NEW_TESTS_SMOOTHED,
                    t.POSITIVE_RATE,

                    /* Hospital */
                    h.ICU_PATIENTS,
                    h.ICU_PATIENTS_PER_MILLION,
                    h.HOSP_PATIENTS,
                    h.HOSP_PATIENTS_PER_MILLION,
                    h.WEEKLY_ICU_ADMISSIONS,
                    h.WEEKLY_HOSP_ADMISSIONS,

                    /* Vaccination */
                    v.NEW_VACCINATIONS,
                    v.TOTAL_VACCINATIONS,
                    v.PEOPLE_VACCINATED,
                    v.PEOPLE_FULLY_VACCINATED,
                    v.TOTAL_BOOSTERS
                  FROM {S}.CASES_DATASET c
                  LEFT JOIN {S}.TESTS_DATASET t
                    ON t.ISO_CODE = c.ISO_CODE AND t.DATE = c.DATE
                  LEFT JOIN {S}.HOSPITAL_DATASET h
                    ON h.ISO_CODE = c.ISO_CODE AND h.DATE = c.DATE
                  LEFT JOIN {S}.VACCINATION_DATASET v
                    ON v.ISO_CODE = c.ISO_CODE AND v.DATE = c.DATE
                  LEFT JOIN {S}.DIM_COUNTRY d
                    ON d.ISO_CODE = c.ISO_CODE
                ),
                metrics AS (
                  SELECT
                    *,
                    /* Rolling windows (7 dias) por país) */
                    SUM(NEW_CASES)  OVER (PARTITION BY ISO_CODE ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NEW_CASES_7D,
                    SUM(NEW_DEATHS) OVER (PARTITION BY ISO_CODE ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NEW_DEATHS_7D,
                    SUM(NEW_TESTS)  OVER (PARTITION BY ISO_CODE ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NEW_TESTS_7D,
                    SUM(NEW_VACCINATIONS) OVER (PARTITION BY ISO_CODE ORDER BY DATE ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS NEW_VACCINATIONS_7D
                  FROM base
                )
                SELECT
                  ISO_CODE, DATE, COUNTRY, CONTINENT, POPULATION,

                  /* Níveis diários e cumulativos */
                  NEW_CASES, TOTAL_CASES, NEW_DEATHS, TOTAL_DEATHS,
                  NEW_TESTS, NEW_TESTS_PER_THOUSAND, NEW_TESTS_SMOOTHED, POSITIVE_RATE,
                  ICU_PATIENTS, ICU_PATIENTS_PER_MILLION, HOSP_PATIENTS, HOSP_PATIENTS_PER_MILLION,
                  WEEKLY_ICU_ADMISSIONS, WEEKLY_HOSP_ADMISSIONS,
                  NEW_VACCINATIONS, TOTAL_VACCINATIONS, PEOPLE_VACCINATED, PEOPLE_FULLY_VACCINATED, TOTAL_BOOSTERS,

                  /* Rolling 7d */
                  NEW_CASES_7D, NEW_DEATHS_7D, NEW_TESTS_7D, NEW_VACCINATIONS_7D,

                  /* Por 100k de hab (diário) */
                  ROUND((NEW_CASES  * 100000.0) / NULLIF(POPULATION, 0), 2) AS NEW_CASES_PER_100K,
                  ROUND((NEW_DEATHS * 100000.0) / NULLIF(POPULATION, 0), 2) AS NEW_DEATHS_PER_100K,
                  ROUND((NEW_VACCINATIONS * 100000.0) / NULLIF(POPULATION, 0), 2) AS NEW_VACCINATIONS_PER_100K,

                  /* Coberturas e taxas */
                  ROUND((PEOPLE_VACCINATED        / NULLIF(POPULATION, 0)) * 100, 2) AS VACCINATED_COVERAGE_PCT,
                  ROUND((PEOPLE_FULLY_VACCINATED  / NULLIF(POPULATION, 0)) * 100, 2) AS FULLY_VACCINATED_COVERAGE_PCT,
                  ROUND((TOTAL_BOOSTERS           / NULLIF(POPULATION, 0)) * 100, 2) AS BOOSTER_COVERAGE_PCT
                FROM metrics;

                ALTER TABLE {G}.FACT_COVID_DAILY CLUSTER BY (ISO_CODE, DATE);
            """,
        },
    )

    create_view_country_latest = PythonOperator(
        task_id="create_view_country_latest",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {GOLD_SCHEMA};
                CREATE OR REPLACE VIEW {G}.V_COUNTRY_LATEST AS
                WITH mx AS (
                  SELECT ISO_CODE, MAX(DATE) AS max_date
                  FROM {G}.FACT_COVID_DAILY
                  GROUP BY ISO_CODE
                )
                SELECT f.*
                FROM {G}.FACT_COVID_DAILY f
                JOIN mx ON mx.ISO_CODE = f.ISO_CODE AND mx.max_date = f.DATE;
            """,
        },
    )

    create_view_trend_country = PythonOperator(
        task_id="create_view_trend_country",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {GOLD_SCHEMA};
                CREATE OR REPLACE VIEW {G}.V_TREND_COUNTRY AS
                SELECT
                  ISO_CODE, COUNTRY, DATE,
                  NEW_CASES, NEW_CASES_7D, NEW_CASES_PER_100K,
                  NEW_DEATHS, NEW_DEATHS_7D, NEW_DEATHS_PER_100K,
                  POSITIVE_RATE,
                  NEW_VACCINATIONS, NEW_VACCINATIONS_7D, NEW_VACCINATIONS_PER_100K
                FROM {G}.FACT_COVID_DAILY;
            """,
        },
    )

    create_view_country_enriched = PythonOperator(
        task_id="create_view_country_enriched",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {GOLD_SCHEMA};

                CREATE OR REPLACE VIEW {G}.V_DIM_COUNTRY_ENRICHED AS
                SELECT
                    c.ISO_CODE,
                    c.POPULATION,
                    r.COUNTRY_NAME AS COUNTRY_API_NAME,
                    r.OFFICIAL_NAME,
                    r.REGION,
                    r.SUBREGION,
                    r.CAPITAL,
                    r.AREA_KM2,
                    r.POPULATION_API,
                    ROUND((r.POPULATION_API - c.POPULATION), 2) AS POPULATION_DIFF,
                    CURRENT_TIMESTAMP() AS CREATED_AT
                FROM {S}.DIM_COUNTRY c
                LEFT JOIN {S}.DIM_RESTCOUNTRIES r
                  ON c.ISO_CODE = r.ISO_CODE;
            """,
        },
    )

    build_country_latest_snapshot = PythonOperator(
        task_id="build_country_latest_snapshot",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {GOLD_SCHEMA};
                /* Cria snapshot robusto do último registro não nulo de vacinação para cada país */
                CREATE OR REPLACE VIEW {G}.V_COUNTRY_LATEST AS
                WITH latest_vax AS (
                  SELECT
                    ISO_CODE,
                    MAX_BY(DATE, CASE WHEN PEOPLE_VACCINATED IS NOT NULL THEN DATE ELSE NULL END) AS LAST_VAX_DATE
                  FROM {G}.FACT_COVID_DAILY
                  GROUP BY ISO_CODE
                ),
                last_values AS (
                  SELECT
                    f.*
                  FROM {G}.FACT_COVID_DAILY f
                  JOIN latest_vax lv
                    ON f.ISO_CODE = lv.ISO_CODE AND f.DATE = lv.LAST_VAX_DATE
                )
                SELECT * FROM last_values;
            """,
        },
    )

    create_gold_schema >> build_fact_covid_daily >> [build_country_latest_snapshot, create_view_country_latest, create_view_trend_country, create_view_country_enriched]
