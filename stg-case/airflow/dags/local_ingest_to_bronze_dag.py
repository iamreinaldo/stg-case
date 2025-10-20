from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os, pandas as pd
import numpy as np
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
BRONZE_DIR = os.getenv("BRONZE_DIR", "/opt/airflow/data_lake/bronze")

SF_CFG = dict(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE", None),
)

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df

def ingest_local_to_parquet():
    import glob
    os.makedirs(BRONZE_DIR, exist_ok=True)

    patterns = ["*.csv", "*.CSV", "*.xlsx", "*.XLSX", "*.xls", "*.XLS"]
    files = []
    for p in patterns:
        files.extend(glob.glob(os.path.join(DATA_DIR, p)))

    print(f"[ingest] DATA_DIR={DATA_DIR} | encontrados={len(files)} arquivo(s)")
    if not files:
        raise ValueError(f"Nenhum arquivo encontrado em {DATA_DIR}. Verifique o volume do docker-compose e a pasta 'data/'.")

    processed = 0

    def read_with_sniff(path, fname):
        # 1) tenta como CSV com autodetecção
        if fname.lower().endswith(".csv"):
            try:
                return pd.read_csv(path, sep=None, engine="python", encoding="utf-8", on_bad_lines="skip")
            except Exception:
                # tenta separador ; explicitamente
                return pd.read_csv(path, sep=";", engine="python", encoding="utf-8", on_bad_lines="skip")

        # 2) tenta como Excel de verdade (arquivo zip PK..)
        if fname.lower().endswith((".xlsx", ".xls")):
            try:
                with open(path, "rb") as fh:
                    sig = fh.read(4)
                if sig == b"PK\x03\x04":  # zip header de XLSX
                    return pd.read_excel(path, engine="openpyxl", header=0)
            except Exception:
                pass
            # 3) fallback: pode ser CSV salvo com extensão .xlsx → reinterpreta como CSV
            try:
                return pd.read_csv(path, sep=None, engine="python", encoding="utf-8", on_bad_lines="skip")
            except Exception:
                return pd.read_csv(path, sep=";", engine="python", encoding="utf-8", on_bad_lines="skip")

        raise ValueError(f"Extensão não suportada: {fname}")

    for path in files:
        fname = os.path.basename(path)
        try:
            df = read_with_sniff(path, fname)
        except Exception as e:
            raise RuntimeError(f"[ingest] falha ao ler {fname}: {e}")

        # corrige coluna de data, se existir
        if 'date' in df.columns:
            s = df['date']
            # tenta parse ISO
            dt = pd.to_datetime(s, errors='coerce', infer_datetime_format=True)
            # tenta converter seriais Excel
            num = pd.to_numeric(s, errors='coerce')
            mask_excel = dt.isna() & num.notna()
            dt.loc[mask_excel] = pd.to_datetime('1899-12-30') + pd.to_timedelta(num[mask_excel], unit='D')
            # filtra intervalo plausível
            min_dt = pd.Timestamp('2019-01-01')
            max_dt = pd.Timestamp('2024-12-31')
            mask_out = (dt < min_dt) | (dt > max_dt)
            dt.loc[mask_out] = pd.NaT
            # salva no formato ISO
            df['date'] = dt.dt.strftime('%Y-%m-%d')

        # Se leu com apenas 1 coluna, tenta reparar (dados 'colados' em uma string)
        if df.shape[1] == 1:
            colname = str(df.columns[0])
            sample = str(df.iloc[0, 0]) if len(df) else ""
            # tenta dividir pelo delimitador mais provável
            for delim in [",", ";", "\t", "|"]:
                if delim in sample or delim in colname:
                    try:
                        # reparse o arquivo como CSV com este delimitador
                        df = pd.read_csv(path, sep=delim, engine="python", encoding="utf-8", on_bad_lines="skip")
                        break
                    except Exception:
                        pass

        # normaliza nomes e salva
        df = _normalize_cols(df)
        out = os.path.join(BRONZE_DIR, os.path.splitext(fname)[0].lower() + ".parquet")
        df.to_parquet(out, index=False)
        print(f"[ingest] salvo: {out} | cols={list(df.columns)}")
        processed += 1

    if processed == 0:
        raise ValueError("[ingest] nenhum arquivo processado.")

def load_parquet_to_snowflake():
    import re, glob
    con = connect(**{k:v for k,v in SF_CFG.items() if v})
    cs = con.cursor()
    with con, cs:
        cs.execute(f"USE DATABASE {SF_CFG['database']}")
        cs.execute(f"USE SCHEMA {SF_CFG['schema']}")
        cs.execute("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("[snowflake ctx]", cs.fetchone())

        files = sorted(glob.glob(os.path.join(BRONZE_DIR, "*.parquet")))
        print(f"[load] BRONZE_DIR={BRONZE_DIR} | {len(files)} parquet(s)")

        if not files:
            raise ValueError(f"Nenhum parquet encontrado em {BRONZE_DIR}. Rode a task local_to_parquet primeiro.")

        def clean_col(c: str) -> str:
            c = str(c).strip().lower().replace(" ", "_")
            c = re.sub(r"[^a-z0-9_]", "_", c) 
            c = re.sub(r"_+", "_", c).strip("_")
            if not c or re.match(r"^[^a-z_]", c): 
                c = f"c_{c or 'col'}"
            c = c[:240] 
            return c.upper()

        for path in files:
            table = os.path.splitext(os.path.basename(path))[0].upper()
            df = pd.read_parquet(path)
            # normaliza nomes de colunas
            new_cols = [clean_col(c) for c in df.columns]
            df.columns = new_cols
            print(f"[load] {table} cols:", new_cols)

            if len(df.columns) == 0:
                # fallback: cria uma coluna única
                df["_RAW"] = None
                df = df[["_RAW"]]
                new_cols = ["_RAW"]

            # inferência simples de tipos
            dtypes = {}
            for c, t in df.dtypes.items():
                t = str(t)
                if t.startswith(("float", "int")):
                    dtypes[c] = "FLOAT"
                elif "datetime64" in t or "datetime" in t:
                    dtypes[c] = "TIMESTAMP_NTZ"
                elif t.startswith("bool"):
                    dtypes[c] = "BOOLEAN"
                else:
                    dtypes[c] = "TEXT"

            cols_sql = ", ".join([f'"{c}" {dtypes[c]}' for c in new_cols])
            create_sql = f'CREATE OR REPLACE TABLE "{table}" ({cols_sql})'
            print("[load] create_sql:", create_sql)
            cs.execute(create_sql)

            # carrega (com nomes já em UPPER e quote_identifiers=True)
            ok, nchunks, nrows, _ = write_pandas(con, df, table_name=table, quote_identifiers=True)
            print(f"[load] {table}: ok={ok} chunks={nchunks} rows={nrows}")


default_args = {"start_date": datetime(2024,1,1)}

with DAG("ingest_to_bronze", schedule=None, catchup=False, default_args=default_args, tags=["bronze"]) as dag:
    to_parquet = PythonOperator(task_id="local_to_parquet", python_callable=ingest_local_to_parquet)
    to_snowflake = PythonOperator(task_id="parquet_to_snowflake", python_callable=load_parquet_to_snowflake)
    to_parquet >> to_snowflake
