"""
=============================================================
 VIDEO GAME SALES — DAG Airflow
 Orchestration complète du pipeline data engineer
=============================================================
 Prérequis :
   pip install apache-airflow apache-airflow-providers-postgres
   Variables Airflow à définir :
     - KAGGLE_USERNAME, KAGGLE_KEY
     - RAWG_API_KEY
   Connection Airflow :
     - postgres_vgsales (conn_id PostgreSQL)
=============================================================
"""

from __future__ import annotations

import os
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python   import PythonOperator
from airflow.operators.bash     import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models             import Variable

# ── Constantes ──────────────────────────────────────────────
POSTGRES_CONN_ID = "postgres_vgsales"
DATA_DIR         = Path("/opt/airflow/data")
RAW_DIR          = DATA_DIR / "raw"
STAGED_DIR       = DATA_DIR / "staged"
DATASET_SLUG     = "asaniczka/video-game-sales-2024"

REGION_MAP = {
    "na_sales":    ("NA",    "North America", "Americas"),
    "pal_sales":   ("EU",    "Europe",        "Europe"),
    "jp_sales":    ("JP",    "Japan",         "Asia"),
    "other_sales": ("Other", "Rest of World", "Various"),
}

log = logging.getLogger(__name__)

# ── Fonctions des tâches ─────────────────────────────────────

def task_download_kaggle(**ctx):
    """Télécharge le dataset Kaggle via la CLI."""
    import subprocess

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    kaggle_user = Variable.get("KAGGLE_USERNAME")
    kaggle_key  = Variable.get("KAGGLE_KEY")

    env = {
        **os.environ,
        "KAGGLE_USERNAME": kaggle_user,
        "KAGGLE_KEY":      kaggle_key,
    }
    cmd = [
        "kaggle", "datasets", "download",
        "-d", DATASET_SLUG,
        "-p", str(RAW_DIR),
        "--unzip",
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Kaggle download failed:\n{result.stderr}")

    log.info("Dataset téléchargé avec succès.")
    ctx["ti"].xcom_push(key="csv_path", value=str(RAW_DIR / "vgchartz-2024.csv"))


def task_clean_and_stage(**ctx):
    """Nettoie le CSV brut et le sauvegarde en Parquet staged."""
    csv_path = ctx["ti"].xcom_pull(task_ids="download_kaggle", key="csv_path")
    STAGED_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path, low_memory=False)
    log.info(f"Lignes brutes : {len(df)}")

    # Normalisation colonnes
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_").str.replace(r"[^\w]", "_", regex=True)
    )

    # Déduplication
    df = df.drop_duplicates(subset=["title"], keep="first")

    # Types numériques
    for col in ["na_sales", "pal_sales", "jp_sales", "other_sales",
                "total_sales", "critic_score", "user_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Année
    if "release_date" in df.columns:
        df["release_year"] = pd.to_datetime(
            df["release_date"], errors="coerce"
        ).dt.year.astype("Int64")

    df = df.dropna(subset=["title"])
    df["platform"] = df.get("console", df.get("platform", "Unknown")).fillna("Unknown")
    df["genre"]    = df.get("genre", pd.Series("Unknown", index=df.index)).fillna("Unknown")

    out = STAGED_DIR / "vgsales_clean.parquet"
    df.to_parquet(out, index=False)
    log.info(f"Staged : {len(df)} lignes → {out}")
    ctx["ti"].xcom_push(key="staged_path", value=str(out))


def task_load_dimensions(**ctx):
    """Charge les dimensions game, genre, platform depuis le staged."""
    staged_path = ctx["ti"].xcom_pull(task_ids="clean_and_stage", key="staged_path")
    df  = pd.read_parquet(staged_path)
    pg  = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur  = conn.cursor()

    # dim_game
    games = df[["title", "publisher", "developer", "critic_score", "user_score"]].drop_duplicates("title")
    for _, row in games.iterrows():
        cur.execute("""
            INSERT INTO dim_game (title, publisher, developer, critic_score, user_score)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (title, publisher) DO UPDATE
              SET critic_score = EXCLUDED.critic_score,
                  user_score   = EXCLUDED.user_score
        """, (
            row.get("title"),
            row.get("publisher"),
            row.get("developer"),
            row.get("critic_score") if pd.notna(row.get("critic_score")) else None,
            row.get("user_score")   if pd.notna(row.get("user_score"))   else None,
        ))

    # dim_genre (genres uniques du dataset)
    genres = df["genre"].dropna().unique()
    for g in genres:
        cur.execute("""
            INSERT INTO dim_genre (genre_name, category)
            VALUES (%s, %s)
            ON CONFLICT (genre_name) DO NOTHING
        """, (g, "Other"))

    conn.commit()
    cur.close()
    log.info("Dimensions chargées.")


def task_load_facts(**ctx):
    """Charge la table de faits en pivotant les colonnes de ventes."""
    staged_path = ctx["ti"].xcom_pull(task_ids="clean_and_stage", key="staged_path")
    df  = pd.read_parquet(staged_path)
    pg  = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur  = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        # Récupérer les IDs FK
        cur.execute("SELECT game_id FROM dim_game WHERE title = %s LIMIT 1", (row["title"],))
        game_row = cur.fetchone()
        if not game_row:
            continue
        game_id = game_row[0]

        cur.execute("SELECT platform_id FROM dim_platform WHERE platform_code = %s LIMIT 1",
                    (str(row["platform"])[:20],))
        plat_row = cur.fetchone()
        platform_id = plat_row[0] if plat_row else None

        cur.execute("SELECT genre_id FROM dim_genre WHERE genre_name = %s LIMIT 1",
                    (str(row.get("genre", "Misc")),))
        genre_row = cur.fetchone()
        genre_id = genre_row[0] if genre_row else None

        total_sales = row.get("total_sales")
        total_sales = float(total_sales) if pd.notna(total_sales) else None

        # Une ligne par région avec ventes > 0
        for col, (region_code, _, _) in REGION_MAP.items():
            val = row.get(col)
            if pd.isna(val) or val <= 0:
                continue

            cur.execute("SELECT region_id FROM dim_region WHERE region_code = %s", (region_code,))
            reg_row = cur.fetchone()
            if not reg_row:
                continue

            cur.execute("""
                INSERT INTO fact_sales
                  (game_id, platform_id, genre_id, region_id, release_year, sales_millions, global_sales)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                game_id, platform_id, genre_id, reg_row[0],
                int(row["release_year"]) if pd.notna(row.get("release_year")) else None,
                round(float(val), 4),
                total_sales,
            ))
            inserted += 1

    conn.commit()
    cur.close()
    log.info(f"Faits insérés : {inserted:,} lignes")


def task_enrich_rawg(**ctx):
    """Enrichit dim_game avec les métadonnées RAWG (top 300 jeux)."""
    staged_path = ctx["ti"].xcom_pull(task_ids="clean_and_stage", key="staged_path")
    df   = pd.read_parquet(staged_path)
    key  = Variable.get("RAWG_API_KEY", default_var=None)
    if not key:
        log.warning("RAWG_API_KEY non définie — enrichissement ignoré.")
        return

    pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur  = conn.cursor()

    top_titles = (
        df.sort_values("total_sales", ascending=False)["title"]
        .dropna().unique()[:300].tolist()
    )

    for i, title in enumerate(top_titles, 1):
        try:
            resp = requests.get(
                "https://api.rawg.io/api/games",
                params={"key": key, "search": title, "page_size": 1},
                timeout=8,
            )
            if resp.status_code == 200:
                data = resp.json().get("results", [])
                if data:
                    g = data[0]
                    esrb = (g.get("esrb_rating") or {}).get("name")
                    tags = ", ".join(x["name"] for x in g.get("tags", [])[:5])
                    cur.execute("""
                        UPDATE dim_game
                        SET rawg_id        = %s,
                            rawg_rating    = %s,
                            rawg_tags      = %s,
                            esrb_rating    = COALESCE(esrb_rating, %s),
                            background_img = %s
                        WHERE title = %s
                    """, (
                        g.get("id"), g.get("rating"), tags, esrb,
                        g.get("background_image"), title,
                    ))
            if i % 5 == 0:
                conn.commit()
                time.sleep(1)
        except Exception as e:
            log.warning(f"RAWG erreur '{title}': {e}")

    conn.commit()
    cur.close()
    log.info("Enrichissement RAWG terminé.")


def task_data_quality(**ctx):
    """Vérifie la qualité des données après chargement."""
    pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur  = conn.cursor()

    checks = {
        "fact_sales non vide":      "SELECT COUNT(*) FROM fact_sales",
        "dim_game non vide":        "SELECT COUNT(*) FROM dim_game",
        "Ventes nulles (anomalie)": "SELECT COUNT(*) FROM fact_sales WHERE sales_millions IS NULL",
        "Jeux sans genre":          "SELECT COUNT(*) FROM fact_sales WHERE genre_id IS NULL",
    }

    all_ok = True
    for label, query in checks.items():
        cur.execute(query)
        count = cur.fetchone()[0]
        status = "✅" if count > 0 or "anomalie" in label else "❌"
        if "anomalie" in label and count > 1000:
            status = "⚠️"
            all_ok = False
        log.info(f"  {status} {label} : {count:,}")

    cur.close()
    if not all_ok:
        raise ValueError("Contrôle qualité échoué — pipeline interrompu.")
    log.info("Contrôle qualité : OK")


# ── Définition du DAG ────────────────────────────────────────
default_args = {
    "owner":            "data_team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past":  False,
}

with DAG(
    dag_id="vgsales_pipeline",
    description="Pipeline complet ventes jeux vidéo : Kaggle → staging → DWH → RAWG",
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bigdata", "gaming", "etl"],
) as dag:

    t0_init = BashOperator(
        task_id="init_directories",
        bash_command=f"mkdir -p {RAW_DIR} {STAGED_DIR}",
    )

    t1_download = PythonOperator(
        task_id="download_kaggle",
        python_callable=task_download_kaggle,
    )

    t2_clean = PythonOperator(
        task_id="clean_and_stage",
        python_callable=task_clean_and_stage,
    )

    t3_dims = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions,
    )

    t4_facts = PythonOperator(
        task_id="load_facts",
        python_callable=task_load_facts,
    )

    t5_rawg = PythonOperator(
        task_id="enrich_rawg",
        python_callable=task_enrich_rawg,
    )

    t6_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=task_data_quality,
    )

    # ── Ordre d'exécution ──────────────────────────────────
    t0_init >> t1_download >> t2_clean >> t3_dims >> t4_facts >> t5_rawg >> t6_quality
