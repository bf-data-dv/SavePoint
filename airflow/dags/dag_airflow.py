"""
=============================================================
 SavePoint — DAG Airflow + Snowflake (version optimisée)
 Fichier : dags/dag_airflow.py

 Corrections appliquées :
   1. Chargement des dimensions en mémoire (dict) → pas de requête par ligne
   2. Gestion robuste des connexions avec try/finally
   3. Colonnes optionnelles gérées avant sélection
=============================================================
"""

from __future__ import annotations

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
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


# ── Helpers ──────────────────────────────────────────────────

def load_source(source: str) -> pd.DataFrame:
    """Charge et normalise le fichier source."""
    df = pd.read_parquet(source) if source.endswith(".parquet") else pd.read_csv(source, low_memory=False)

    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_").str.replace(r"[^\w]", "_", regex=True)
    )

    # Colonnes optionnelles
    for col in ["critic_score", "user_score"]:
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")

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
    return df


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
        raise FileNotFoundError(f"Aucun fichier source trouvé dans {DATA_DIR}")


def task_load_dimensions(**ctx):
    """Charge DIM_GAME et DIM_GENRE en batch avec MERGE."""
    source = ctx["ti"].xcom_pull(task_ids="check_files", key="source")
    df = load_source(source)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    try:
        # DIM_GAME — batch par 500
        games = df[["title", "publisher", "developer", "critic_score", "user_score"]].drop_duplicates("title")
        rows_game = [
            (
                row["title"], row["title"],
                row.get("publisher") if pd.notna(row.get("publisher")) else None,
                row.get("developer") if pd.notna(row.get("developer")) else None,
                float(row["critic_score"]) if pd.notna(row.get("critic_score")) else None,
                float(row["user_score"])   if pd.notna(row.get("user_score"))   else None,
            )
            for _, row in games.iterrows()
        ]
        for i in range(0, len(rows_game), 5000):
            batch = rows_game[i:i+500]
            cur.executemany("""
                MERGE INTO DIM_GAME tgt
                USING (SELECT %s AS TITLE) src ON tgt.TITLE = src.TITLE
                WHEN NOT MATCHED THEN INSERT
                    (TITLE, PUBLISHER, DEVELOPER, CRITIC_SCORE, USER_SCORE)
                VALUES (%s, %s, %s, %s, %s)
            """, batch)
            log.info(f"  DIM_GAME batch {i//500+1} : {len(batch)} lignes")

        # DIM_GENRE
        genres = [(str(g), str(g)) for g in df["genre"].dropna().unique()]
        cur.executemany("""
            MERGE INTO DIM_GENRE tgt
            USING (SELECT %s AS GENRE_NAME) src ON tgt.GENRE_NAME = src.GENRE_NAME
            WHEN NOT MATCHED THEN INSERT (GENRE_NAME, CATEGORY) VALUES (%s, 'Other')
        """, genres)

        conn.commit()
        log.info(f"✅ DIM_GAME : {len(rows_game)} jeux | DIM_GENRE : {len(genres)} genres")

    except Exception as e:
        conn.rollback()
        log.error(f"Erreur load_dimensions : {e}")
        raise
    finally:
        cur.close()
        conn.close()


def task_load_facts(**ctx):
    """
    Charge FACT_SALES.
    Correction #1 : chargement des IDs en mémoire (dict) pour éviter
    30 000+ requêtes SQL individuelles.
    """
    source = ctx["ti"].xcom_pull(task_ids="check_files", key="source")
    df = load_source(source)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    try:
        # ── Chargement des maps d'IDs en mémoire (une seule requête chacune) ──
        log.info("Chargement des maps d'IDs en mémoire...")

        cur.execute("SELECT TITLE, GAME_ID FROM DIM_GAME")
        game_map = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT PLATFORM_CODE, PLATFORM_ID FROM DIM_PLATFORM")
        plat_map = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT GENRE_NAME, GENRE_ID FROM DIM_GENRE")
        genre_map = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT REGION_CODE, REGION_ID FROM DIM_REGION")
        region_map = {row[0]: row[1] for row in cur.fetchall()}

        log.info(f"  Maps : {len(game_map)} jeux | {len(plat_map)} plateformes | {len(genre_map)} genres")

        # ── Construction des lignes en mémoire ──────────────────────────────
        rows = []
        for _, row in df.iterrows():
            game_id     = game_map.get(str(row["title"]))
            platform_id = plat_map.get(str(row["platform"])[:20])
            genre_id    = genre_map.get(str(row.get("genre", "Misc")))
            total       = float(row["total_sales"]) if pd.notna(row.get("total_sales")) else None
            year        = int(row["release_year"])  if pd.notna(row.get("release_year")) else None

            if not game_id:
                continue

            for col, region_code in REGION_MAP.items():
                val = row.get(col)
                if pd.isna(val) or val <= 0:
                    continue
                region_id = region_map.get(region_code)
                if not region_id:
                    continue
                rows.append((game_id, platform_id, genre_id, region_id,
                             year, round(float(val), 4), total))

        log.info(f"  {len(rows):,} lignes FACT_SALES préparées en mémoire")

        # ── Insertion par batch de 500 ───────────────────────────────────────
        for i in range(0, len(rows), 500):
            batch = rows[i:i+500]
            cur.executemany("""
                INSERT INTO FACT_SALES
                    (GAME_ID, PLATFORM_ID, GENRE_ID, REGION_ID,
                     RELEASE_YEAR, SALES_MILLIONS, GLOBAL_SALES)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, batch)
            log.info(f"  Batch {i//500+1} : {len(batch)} lignes insérées")

        conn.commit()
        log.info(f"✅ FACT_SALES : {len(rows):,} lignes insérées")

    except Exception as e:
        conn.rollback()
        log.error(f"Erreur load_facts : {e}")
        raise
    finally:
        cur.close()
        conn.close()


def task_data_quality(**ctx):
    """Contrôles qualité post-chargement dans Snowflake."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur  = conn.cursor()

    try:
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

        log.info("✅ Contrôles qualité terminés")

    finally:
        cur.close()
        conn.close()


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