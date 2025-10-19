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
# DAG
# -----------------------------------------------------------------------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="bronze_to_silver",
    description="Transforma tabelas BRONZE em SILVER no Snowflake (padrão Bronze)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["covid", "silver", "snowflake"],
):
    create_schema = PythonOperator(
        task_id="create_silver_schema",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA};
                USE SCHEMA {SILVER_SCHEMA};
            """,
        },
    )

    silver_cases = PythonOperator(
        task_id="silver_cases",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {SILVER_SCHEMA};
                CREATE OR REPLACE TABLE {S}.CASES_DATASET AS
                SELECT
                    UPPER(TRIM(ISO_CODE))                                  AS ISO_CODE,
                    TRY_TO_DATE(DATE)                                      AS DATE,
                    UPPER(TRIM(CONTINENT))                                 AS CONTINENT,
                    UPPER(TRIM(LOCATION))                                  AS COUNTRY,
                    TRY_TO_DECIMAL(TO_VARCHAR(TOTAL_CASES))               AS TOTAL_CASES,
                    TRY_TO_DECIMAL(TO_VARCHAR(NEW_CASES))                 AS NEW_CASES,
                    TRY_TO_DECIMAL(TO_VARCHAR(TOTAL_DEATHS))              AS TOTAL_DEATHS,
                    TRY_TO_DECIMAL(TO_VARCHAR(NEW_DEATHS))                AS NEW_DEATHS,
                    TRY_TO_DOUBLE(TO_VARCHAR(TOTAL_CASES_PER_MILLION))    AS TOTAL_CASES_PM,
                    TRY_TO_DOUBLE(TO_VARCHAR(TOTAL_DEATHS_PER_MILLION))   AS TOTAL_DEATHS_PM,
                    CURRENT_TIMESTAMP()                                    AS _INGESTED_AT
                FROM {B}.CASES_DATASET
                WHERE ISO_CODE IS NOT NULL AND TRY_TO_DATE(DATE) IS NOT NULL AND UPPER(ISO_CODE) NOT LIKE 'OWID_%';
            """,
        },
    )

    silver_tests = PythonOperator(
        task_id="silver_tests",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {SILVER_SCHEMA};
                CREATE OR REPLACE TABLE {S}.TESTS_DATASET AS
                SELECT
                    UPPER(TRIM(ISO_CODE))                                 AS ISO_CODE,
                    TRY_TO_DATE(DATE)                                     AS DATE,
                    TRY_TO_DECIMAL(TO_VARCHAR(TOTAL_TESTS))              AS TOTAL_TESTS,
                    TRY_TO_DECIMAL(TO_VARCHAR(NEW_TESTS))                AS NEW_TESTS,
                    TRY_TO_DOUBLE(TO_VARCHAR(NEW_TESTS_PER_THOUSAND))    AS NEW_TESTS_PER_THOUSAND,
                    TRY_TO_DOUBLE(TO_VARCHAR(NEW_TESTS_SMOOTHED))        AS NEW_TESTS_SMOOTHED,
                    TRY_TO_DOUBLE(TO_VARCHAR(POSITIVE_RATE))             AS POSITIVE_RATE,
                    UPPER(TRIM(TESTS_UNITS))                              AS TESTS_UNITS,
                    CURRENT_TIMESTAMP()                                   AS _INGESTED_AT
                FROM {B}.TESTS_DATASET
                WHERE ISO_CODE IS NOT NULL AND TRY_TO_DATE(DATE) IS NOT NULL AND UPPER(ISO_CODE) NOT LIKE 'OWID_%';
            """,
        },
    )

    silver_hospital = PythonOperator(
        task_id="silver_hospital",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {SILVER_SCHEMA};
                CREATE OR REPLACE TABLE {S}.HOSPITAL_DATASET AS
                SELECT
                    UPPER(TRIM(ISO_CODE))                                  AS ISO_CODE,
                    TRY_TO_DATE(DATE)                                      AS DATE,
                    TRY_TO_DECIMAL(TO_VARCHAR(ICU_PATIENTS))               AS ICU_PATIENTS,
                    TRY_TO_DOUBLE(TO_VARCHAR(ICU_PATIENTS_PER_MILLION))    AS ICU_PATIENTS_PER_MILLION,
                    TRY_TO_DECIMAL(TO_VARCHAR(HOSP_PATIENTS))              AS HOSP_PATIENTS,
                    TRY_TO_DOUBLE(TO_VARCHAR(HOSP_PATIENTS_PER_MILLION))   AS HOSP_PATIENTS_PER_MILLION,
                    TRY_TO_DOUBLE(TO_VARCHAR(WEEKLY_ICU_ADMISSIONS))       AS WEEKLY_ICU_ADMISSIONS,
                    TRY_TO_DOUBLE(TO_VARCHAR(WEEKLY_HOSP_ADMISSIONS))      AS WEEKLY_HOSP_ADMISSIONS,
                    CURRENT_TIMESTAMP()                                     AS _INGESTED_AT
                FROM {B}.HOSPITAL_DATASET
                WHERE ISO_CODE IS NOT NULL AND TRY_TO_DATE(DATE) IS NOT NULL AND UPPER(ISO_CODE) NOT LIKE 'OWID_%';
            """,
        },
    )

    silver_vaccination = PythonOperator(
        task_id="silver_vaccination",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {SILVER_SCHEMA};
                CREATE OR REPLACE TABLE {S}.VACCINATION_DATASET AS
                SELECT
                    UPPER(TRIM(ISO_CODE))                                  AS ISO_CODE,
                    TRY_TO_DATE(DATE)                                      AS DATE,
                    TRY_TO_DECIMAL(TO_VARCHAR(TOTAL_VACCINATIONS))         AS TOTAL_VACCINATIONS,
                    TRY_TO_DECIMAL(TO_VARCHAR(PEOPLE_VACCINATED))          AS PEOPLE_VACCINATED,
                    TRY_TO_DECIMAL(TO_VARCHAR(PEOPLE_FULLY_VACCINATED))    AS PEOPLE_FULLY_VACCINATED,
                    TRY_TO_DECIMAL(TO_VARCHAR(TOTAL_BOOSTERS))             AS TOTAL_BOOSTERS,
                    TRY_TO_DECIMAL(TO_VARCHAR(NEW_VACCINATIONS))           AS NEW_VACCINATIONS,
                    CURRENT_TIMESTAMP()                                     AS _INGESTED_AT
                FROM {B}.VACCINATION_DATASET
                WHERE ISO_CODE IS NOT NULL AND TRY_TO_DATE(DATE) IS NOT NULL AND UPPER(ISO_CODE) NOT LIKE 'OWID_%';
            """,
        },
    )

    silver_dim_country = PythonOperator(
        task_id="silver_dim_country",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {SILVER_SCHEMA};
                CREATE OR REPLACE TABLE {S}.DIM_COUNTRY AS
                SELECT
                    UPPER(TRIM(ISO_CODE))                                      AS ISO_CODE,
                    TRY_TO_DECIMAL(TO_VARCHAR(POPULATION))                    AS POPULATION,
                    TRY_TO_DOUBLE(TO_VARCHAR(MEDIAN_AGE))                     AS MEDIAN_AGE,
                    TRY_TO_DOUBLE(TO_VARCHAR(HOSPITAL_BEDS_PER_THOUSAND))     AS HOSPITAL_BEDS_PER_THOUSAND,
                    TRY_TO_DOUBLE(TO_VARCHAR(LIFE_EXPECTANCY))                AS LIFE_EXPECTANCY,
                    TRY_TO_DOUBLE(TO_VARCHAR(GDP_PER_CAPITA))                 AS GDP_PER_CAPITA,
                    TRY_TO_DOUBLE(TO_VARCHAR(DIABETES_PREVALENCE))            AS DIABETES_PREVALENCE,
                    TRY_TO_DOUBLE(TO_VARCHAR(HUMAN_DEVELOPMENT_INDEX))        AS HDI,
                    CURRENT_TIMESTAMP()                                        AS _INGESTED_AT
                FROM {B}.COUNTRY_DATASET
                WHERE ISO_CODE IS NOT NULL;
            """,
        },
    )

    silver_dim_restcountries = PythonOperator(
        task_id="silver_dim_restcountries",
        python_callable=run_sql,
        op_kwargs={
            "sql": f"""
                USE DATABASE {DB_NAME};
                USE SCHEMA {SILVER_SCHEMA};
                CREATE OR REPLACE TABLE {S}.DIM_RESTCOUNTRIES AS
                SELECT
                    UPPER(TRIM(ISO_CODE))                            AS ISO_CODE,
                    UPPER(TRIM(COUNTRY_NAME))                        AS COUNTRY_NAME,
                    UPPER(TRIM(OFFICIAL_NAME))                       AS OFFICIAL_NAME,
                    UPPER(TRIM(REGION))                              AS REGION,
                    UPPER(TRIM(SUBREGION))                           AS SUBREGION,
                    UPPER(TRIM(CAPITAL))                             AS CAPITAL,
                    TRY_TO_DOUBLE(TO_VARCHAR(AREA))                  AS AREA_KM2,
                    TRY_TO_DOUBLE(TO_VARCHAR(LAT))                    AS LAT,
                    TRY_TO_DOUBLE(TO_VARCHAR(LON))                    AS LON,
                    LANGUAGES                                        AS LANGUAGES,
                    CURRENCIES                                       AS CURRENCIES,
                    TRY_TO_DECIMAL(TO_VARCHAR(POPULATION_API))       AS POPULATION_API,
                    CURRENT_TIMESTAMP()                              AS _INGESTED_AT
                FROM {B}.RESTCOUNTRIES_RAW;
            """,
        },
    )

    create_schema >> [
        silver_cases,
        silver_tests,
        silver_hospital,
        silver_vaccination,
        silver_dim_country,
        silver_dim_restcountries,
    ]