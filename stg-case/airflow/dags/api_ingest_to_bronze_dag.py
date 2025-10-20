

import os
import json
import requests
import pandas as pd
from datetime import datetime

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow import DAG
from airflow.operators.python import PythonOperator

from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# -----------------------------------------------------------------------------
# Configs gerais (mesmo padrão do Bronze)
# -----------------------------------------------------------------------------
SF_CFG = dict(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE", None),
)

DB_NAME = os.getenv("COVID_DB_NAME", "COVID_DB")
BRONZE_SCHEMA = os.getenv("COVID_BRONZE_SCHEMA", "BRONZE")
API_PARQUET_PATH = os.getenv("API_PARQUET_PATH", "/opt/airflow/data/api/restcountries.parquet")

B = f"{DB_NAME}.{BRONZE_SCHEMA}"

# -----------------------------------------------------------------------------
# 1) Task: Buscar REST Countries e salvar Parquet
# -----------------------------------------------------------------------------
RESTCOUNTRIES_URL = "https://restcountries.com/v3.1/all"
RESTCOUNTRIES_FIELDS = [
    "cca3",
    "name",
    "region",
    "subregion",
    "capital",
    "area",
    "latlng",
    "languages",
    "currencies",
    "population",
]


# Helper para criar sessão HTTP resiliente
def _http_session():
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "case-engineer-airflow/1.0",
        "Accept": "application/json",
    })
    return s

def fetch_restcountries_to_parquet():
    os.makedirs(os.path.dirname(API_PARQUET_PATH), exist_ok=True)

    sess = _http_session()
    params = {"fields": ",".join(RESTCOUNTRIES_FIELDS)}
    try:
        r = sess.get(RESTCOUNTRIES_URL, params=params, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        # Fallback: tenta sem filtro de fields
        r = sess.get(RESTCOUNTRIES_URL, timeout=30)
        try:
            r.raise_for_status()
        except Exception:
            # Loga trecho da resposta para facilitar debug e relança
            snippet = r.text[:500] if hasattr(r, "text") else str(e)
            raise RuntimeError(f"RESTCountries request failed (status={r.status_code}). Body snippet: {snippet}") from e
    data = r.json()

    # Normaliza JSON
    df = pd.json_normalize(data)

    # Seleciona e mapeia campos úteis
    out = pd.DataFrame({
        "ISO_CODE": df.get("cca3", pd.Series()).astype(str).str.upper(),
        "COUNTRY_NAME": df.get("name.common", pd.Series()),
        "OFFICIAL_NAME": df.get("name.official", pd.Series()),
        "REGION": df.get("region", pd.Series()),
        "SUBREGION": df.get("subregion", pd.Series()),
        "CAPITAL": df.get("capital", pd.Series()).apply(lambda x: x[0] if isinstance(x, list) and x else None),
        "AREA": pd.to_numeric(df.get("area", pd.Series()), errors="coerce"),
        "LAT": df.get("latlng", pd.Series()).apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None),
        "LON": df.get("latlng", pd.Series()).apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None),
        "LANGUAGES": df.get("languages", pd.Series()).apply(lambda d: ",".join(d.values()) if isinstance(d, dict) else None),
        "CURRENCIES": df.get("currencies", pd.Series()).apply(lambda d: ",".join(d.keys()) if isinstance(d, dict) else None),
        "POPULATION_API": pd.to_numeric(df.get("population", pd.Series()), errors="coerce"),
    })

    out = out.dropna(subset=["ISO_CODE"]).reset_index(drop=True)


    out.to_parquet(API_PARQUET_PATH, index=False)

# -----------------------------------------------------------------------------
# 2) Task: Carregar Parquet → Snowflake (BRONZE.RESTCOUNTRIES_RAW)
# -----------------------------------------------------------------------------

def load_parquet_to_snowflake():
    # Lê o parquet recém gerado
    df = pd.read_parquet(API_PARQUET_PATH)

    # Conecta no Snowflake (autocommit para CTAS/DDL implícito do write_pandas)
    cfg = {k: v for k, v in SF_CFG.items() if v}
    with connect(autocommit=True, **cfg) as con:
        with con.cursor() as cs:
            # Garante DB/Schema
            cs.execute(f"USE DATABASE {DB_NAME}")
            cs.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
            cs.execute(f"USE SCHEMA {BRONZE_SCHEMA}")

        # Escreve DataFrame na tabela alvo; cria/replace tabela
        # Observação: write_pandas suporta auto_create_table e overwrite em versões recentes.
        # Caso sua versão não suporte overwrite, a tabela será append; ajuste conforme necessário.
        write_pandas(
            conn=con,
            df=df,
            table_name="RESTCOUNTRIES_RAW",
            database=DB_NAME,
            schema=BRONZE_SCHEMA,
            auto_create_table=True,
            overwrite=True,
        )

# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="api_ingest_to_bronze",
    description="Busca REST Countries → Parquet e carrega no Snowflake (BRONZE)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["api", "restcountries", "bronze", "snowflake"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_restcountries_to_parquet",
        python_callable=fetch_restcountries_to_parquet,
    )

    load_task = PythonOperator(
        task_id="parquet_to_snowflake_bronze",
        python_callable=load_parquet_to_snowflake,
    )

    fetch_task >> load_task