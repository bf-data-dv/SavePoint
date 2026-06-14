"""
=============================================================
 SavePoint — Ingestion vers Snowflake (batch optimisé v3)
 Fichier : ingestion/ingestion_snowflake.py
=============================================================
"""

import os
import logging
import pandas as pd
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "database":  os.getenv("SNOWFLAKE_DATABASE",  "SAVEPOINT"),
    "schema":    os.getenv("SNOWFLAKE_SCHEMA",     "PUBLIC"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE",  "COMPUTE_WH"),
    "role":      os.getenv("SNOWFLAKE_ROLE",       "ACCOUNTADMIN"),
}

ENRICHED_PARQUET = Path("data/staged/vgsales_enriched.parquet")
RAW_CSV          = Path("data/raw/vgchartz-2024.csv")

REGION_MAP = {
    "na_sales":    "NA",
    "pal_sales":   "EU",
    "jp_sales":    "JP",
    "other_sales": "Other",
}

BATCH_SIZE = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_connection():
    log.info("Connexion à Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    log.info("  ✅ Connecté !")
    return conn


def load_data() -> pd.DataFrame:
    if ENRICHED_PARQUET.exists():
        log.info(f"Chargement Parquet enrichi : {ENRICHED_PARQUET}")
        df = pd.read_parquet(ENRICHED_PARQUET)
    else:
        log.info(f"Chargement CSV brut : {RAW_CSV}")
        df = pd.read_csv(RAW_CSV, low_memory=False)

    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_").str.replace(r"[^\w]", "_", regex=True)
    )

    for col in ["na_sales", "pal_sales", "jp_sales", "other_sales", "total_sales", "critic_score"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["user_score"]:
        if col not in df.columns:
            df[col] = None

    if "release_date" in df.columns:
        df["release_year"] = pd.to_datetime(
            df["release_date"], errors="coerce"
        ).dt.year.astype("Int64")

    df = df.drop_duplicates(subset=["title"]).dropna(subset=["title"])
    df["platform"] = df.get("console", df.get("platform", "Unknown")).fillna("Unknown")
    df["genre"]    = df.get("genre", pd.Series("Unknown", index=df.index)).fillna("Unknown")

    log.info(f"  → {len(df):,} jeux chargés")
    return df


def insert_batches(cur, conn, sql: str, rows: list, label: str):
    """Insère des lignes par batch avec progression."""
    total = len(rows)
    if total == 0:
        log.info(f"  ⚠️ {label} : aucune ligne à insérer")
        return

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        cur.executemany(sql, batch)
        conn.commit()
        pct = min(100, round((i + len(batch)) / total * 100))
        log.info(f"  [{label}] {i + len(batch):,}/{total:,} lignes ({pct}%)")

    log.info(f"  ✅ {label} terminé : {total:,} lignes")


def load_dim_game(cur, conn, df: pd.DataFrame):
    log.info("━━━ DIM_GAME ━━━")
    games = df[["title", "publisher", "developer", "critic_score", "user_score"]].drop_duplicates("title")

    rows = [
        (
            str(r["title"])[:300],
            str(r["publisher"])[:200] if pd.notna(r.get("publisher")) else None,
            str(r["developer"])[:200] if pd.notna(r.get("developer")) else None,
            float(r["critic_score"])  if pd.notna(r.get("critic_score")) else None,
            float(r["user_score"])    if pd.notna(r.get("user_score"))   else None,
        )
        for _, r in games.iterrows()
    ]

    sql = """
        INSERT INTO DIM_GAME (TITLE, PUBLISHER, DEVELOPER, CRITIC_SCORE, USER_SCORE)
        VALUES (%s, %s, %s, %s, %s)
    """
    insert_batches(cur, conn, sql, rows, "DIM_GAME")


def load_dim_genre(cur, conn, df: pd.DataFrame):
    log.info("━━━ DIM_GENRE ━━━")
    genres = df["genre"].dropna().unique()
    rows = [(str(g), "Other") for g in genres]

    sql = """
        INSERT INTO DIM_GENRE (GENRE_NAME, CATEGORY)
        VALUES (%s, %s)
    """
    insert_batches(cur, conn, sql, rows, "DIM_GENRE")


def load_fact_sales(cur, conn, df: pd.DataFrame):
    log.info("━━━ FACT_SALES ━━━")
    log.info("  Chargement des maps d'IDs en mémoire...")

    cur.execute("SELECT TITLE, GAME_ID FROM DIM_GAME")
    game_map = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT PLATFORM_CODE, PLATFORM_ID FROM DIM_PLATFORM")
    plat_map = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT GENRE_NAME, GENRE_ID FROM DIM_GENRE")
    genre_map = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT REGION_CODE, REGION_ID FROM DIM_REGION")
    region_map = {row[0]: row[1] for row in cur.fetchall()}

    log.info(f"  Maps : {len(game_map)} jeux | {len(plat_map)} plateformes | {len(genre_map)} genres")

    rows = []
    for _, row in df.iterrows():
        game_id     = game_map.get(str(row["title"])[:300])
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
            rows.append((game_id, platform_id, genre_id, region_id, year,
                         round(float(val), 4), total))

    log.info(f"  {len(rows):,} lignes FACT_SALES préparées en mémoire")

    sql = """
        INSERT INTO FACT_SALES
            (GAME_ID, PLATFORM_ID, GENRE_ID, REGION_ID,
             RELEASE_YEAR, SALES_MILLIONS, GLOBAL_SALES)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    insert_batches(cur, conn, sql, rows, "FACT_SALES")


def run():
    missing = [k for k, v in SNOWFLAKE_CONFIG.items() if not v]
    if missing:
        raise ValueError(f"Variables manquantes dans .env : {missing}")

    conn = get_connection()
    cur  = conn.cursor()

    try:
        cur.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cur.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        cur.execute(f"USE WAREHOUSE {SNOWFLAKE_CONFIG['warehouse']}")

        log.info("Nettoyage des tables...")
        cur.execute("TRUNCATE TABLE IF EXISTS FACT_SALES")
        cur.execute("TRUNCATE TABLE IF EXISTS DIM_GAME")
        cur.execute("TRUNCATE TABLE IF EXISTS DIM_GENRE")
        conn.commit()
        log.info("  ✅ Tables vidées")

        df = load_data()

        load_dim_game(cur, conn, df)
        load_dim_genre(cur, conn, df)
        load_fact_sales(cur, conn, df)

        log.info("\n=== RÉSUMÉ FINAL ===")
        for table in ["DIM_GAME", "DIM_GENRE", "DIM_PLATFORM", "DIM_REGION", "FACT_SALES"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            log.info(f"  {table:<20} : {cur.fetchone()[0]:>10,} lignes")

        log.info("✅ Pipeline terminé avec succès !")

    except Exception as e:
        conn.rollback()
        log.error(f"Erreur pipeline : {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run()
