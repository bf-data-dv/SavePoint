"""
=============================================================
 SavePoint — DAG Airflow + Snowflake
 Fichier : dags/dag_airflow.py
=============================================================
 Prérequis :
   pip install apache-airflow
              apache-airflow-providers-snowflake
              snowflake-connector-python
              pandas pyarrow python-dotenv

 Connection Airflow à créer :
   airflow connections add snowflake_savepoint \
     --conn-type snowflake \
     --conn-host UL17173.snowflakecomputing.com \
     --conn-login BFDATADV \
     --conn-password TON_MOT_DE_PASSE \
     --conn-schema PUBLIC \
     --conn-extra '{"database": "SAVEPOINT", "warehouse": "COMPUTE_WH", "role": "ACCOUNTADMIN"}'
=============================================================
"""

from __future__ import annotations

import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python        import PythonOperator
from airflow.operators.bash          import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_savepoint"
DATA_DIR          = Path("/opt/airflow/data")
RAW_CSV           = DATA_DIR / "raw/vgchartz-2024.csv"
ENRICHED_PARQUET  = DATA_DIR / "staged/vgsales_enriched.parquet"

REGION_MAP = {
    "na_sales":    "NA",
    "pal_sales":   "EU",
    "jp_sales":    "JP",
    "other_sales": "Other",
}

log = logging.getLogger(__name__)


# ── Tâches ──────────────────────────────────────────────────

def task_check_files(**ctx):
    """Vérifie que les fichiers source existent."""
    if ENRICHED_PARQUET.exists():
        log.info(f"✅ Parquet enrichi trouvé : {ENRICHED_PARQUET}")
        ctx["ti"].xcom_push(key="source", value=str(ENRICHED_PARQUET))
    elif RAW_CSV.exists():
        log.info(f"✅ CSV brut trouvé : {RAW_CSV}")
        ctx["ti"].xcom_push(key="source", value=str(RAW_CSV))
    else:
        raise FileNotFoundError(
            f"Aucun fichier source trouvé dans {DATA_DIR}/raw/ ou staged/"
        )


def task_load_dimensions(**ctx):
    """Charge DIM_GAME et DIM_GENRE depuis le fichier source."""
    source = ctx["ti"].xcom_pull(task_ids="check_files", key="source")

    if source.endswith(".parquet"):
        df = pd.read_parquet(source)
    else:
        df = pd.read_csv(source, low_memory=False)

    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_").str.replace(r"[^\w]", "_", regex=True)
    )
    for col in ["critic_score", "user_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates(subset=["title"]).dropna(subset=["title"])
    df["platform"] = df.get("console", df.get("platform", "Unknown")).fillna("Unknown")
    df["genre"]    = df.get("genre", pd.Series("Unknown", index=df.index)).fillna("Unknown")

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    # DIM_GAME
    games = df[["title", "publisher", "developer", "critic_score", "user_score"]].drop_duplicates("title")
    for _, row in games.iterrows():
        cur.execute("""
            MERGE INTO DIM_GAME tgt
            USING (SELECT %s AS TITLE) src ON tgt.TITLE = src.TITLE
            WHEN NOT MATCHED THEN INSERT
                (TITLE, PUBLISHER, DEVELOPER, CRITIC_SCORE, USER_SCORE)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row["title"], row["title"],
            row.get("publisher") if pd.notna(row.get("publisher")) else None,
            row.get("developer") if pd.notna(row.get("developer")) else None,
            float(row["critic_score"]) if pd.notna(row.get("critic_score")) else None,
            float(row["user_score"])   if pd.notna(row.get("user_score"))   else None,
        ))

    # DIM_GENRE
    for g in df["genre"].dropna().unique():
        cur.execute("""
            MERGE INTO DIM_GENRE tgt
            USING (SELECT %s AS GENRE_NAME) src ON tgt.GENRE_NAME = src.GENRE_NAME
            WHEN NOT MATCHED THEN INSERT (GENRE_NAME, CATEGORY) VALUES (%s, 'Other')
        """, (g, g))

    conn.commit()
    cur.close()
    log.info("✅ Dimensions chargées")


def task_load_facts(**ctx):
    """Charge FACT_SALES avec pivot des colonnes de ventes."""
    source = ctx["ti"].xcom_pull(task_ids="check_files", key="source")

    if source.endswith(".parquet"):
        df = pd.read_parquet(source)
    else:
        df = pd.read_csv(source, low_memory=False)

    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_").str.replace(r"[^\w]", "_", regex=True)
    )
    for col in list(REGION_MAP.keys()) + ["total_sales"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "release_date" in df.columns:
        df["release_year"] = pd.to_datetime(
            df["release_date"], errors="coerce"
        ).dt.year.astype("Int64")

    df = df.drop_duplicates(subset=["title"]).dropna(subset=["title"])
    df["platform"] = df.get("console", df.get("platform", "Unknown")).fillna("Unknown")
    df["genre"]    = df.get("genre", pd.Series("Unknown", index=df.index)).fillna("Unknown")

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    rows = []
    for _, row in df.iterrows():
        cur.execute("SELECT GAME_ID FROM DIM_GAME WHERE TITLE = %s LIMIT 1", (row["title"],))
        g = cur.fetchone()
        if not g:
            continue

        cur.execute("SELECT PLATFORM_ID FROM DIM_PLATFORM WHERE PLATFORM_CODE = %s LIMIT 1",
                    (str(row["platform"])[:20],))
        p = cur.fetchone()

        cur.execute("SELECT GENRE_ID FROM DIM_GENRE WHERE GENRE_NAME = %s LIMIT 1",
                    (str(row.get("genre", "Misc")),))
        ge = cur.fetchone()

        total = float(row["total_sales"]) if pd.notna(row.get("total_sales")) else None
        year  = int(row["release_year"])  if pd.notna(row.get("release_year")) else None

        for col, region_code in REGION_MAP.items():
            val = row.get(col)
            if pd.isna(val) or val <= 0:
                continue
            cur.execute("SELECT REGION_ID FROM DIM_REGION WHERE REGION_CODE = %s", (region_code,))
            r = cur.fetchone()
            if r:
                rows.append((g[0], p[0] if p else None, ge[0] if ge else None,
                             r[0], year, round(float(val), 4), total))

    # Batch insert
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        cur.executemany("""
            INSERT INTO FACT_SALES
                (GAME_ID, PLATFORM_ID, GENRE_ID, REGION_ID,
                 RELEASE_YEAR, SALES_MILLIONS, GLOBAL_SALES)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, batch)
        log.info(f"  Batch {i//500 + 1} : {len(batch)} lignes insérées")

    conn.commit()
    cur.close()
    log.info(f"✅ FACT_SALES : {len(rows):,} lignes insérées")


def task_data_quality(**ctx):
    """Contrôles qualité post-chargement dans Snowflake."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    checks = {
        "FACT_SALES non vide":   "SELECT COUNT(*) FROM FACT_SALES",
        "DIM_GAME non vide":     "SELECT COUNT(*) FROM DIM_GAME",
        "Ventes nulles":         "SELECT COUNT(*) FROM FACT_SALES WHERE SALES_MILLIONS IS NULL",
        "Jeux sans région":      "SELECT COUNT(*) FROM FACT_SALES WHERE REGION_ID IS NULL",
    }

    for label, query in checks.items():
        cur.execute(query)
        count = cur.fetchone()[0]
        log.info(f"  {'✅' if count > 0 else '⚠️'} {label} : {count:,}")

    cur.close()
    log.info("✅ Contrôles qualité terminés")


# ── Définition du DAG ────────────────────────────────────────
default_args = {
    "owner":            "brahim",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="savepoint_snowflake_pipeline",
    description="SavePoint : pipeline ventes jeux vidéo → Snowflake",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["savepoint", "snowflake", "gaming", "etl"],
) as dag:

    t0 = BashOperator(
        task_id="init_directories",
        bash_command="mkdir -p /opt/airflow/data/raw /opt/airflow/data/staged",
    )

    t1 = PythonOperator(
        task_id="check_files",
        python_callable=task_check_files,
    )

    t2 = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions,
    )

    t3 = PythonOperator(
        task_id="load_facts",
        python_callable=task_load_facts,
    )

    t4 = PythonOperator(
        task_id="data_quality_check",
        python_callable=task_data_quality,
    )

    t0 >> t1 >> t2 >> t3 >> t4
