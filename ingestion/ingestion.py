"""
=============================================================
 VIDEO GAME SALES — Pipeline d'ingestion
 Source : Kaggle (asaniczka/video-game-sales-2024) + RAWG API
=============================================================
"""

import os
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────
RAW_DIR    = Path("data/raw")
STAGED_DIR = Path("data/staged")
RAWG_API_KEY = os.getenv("RAWG_API_KEY", "YOUR_RAWG_API_KEY")  # rawg.io/apidocs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Table de mapping régions → continents ───────────────────
REGION_CONTINENT_MAP = {
    "NA":    {"label": "North America", "continent": "Americas"},
    "EU":    {"label": "Europe",        "continent": "Europe"},
    "JP":    {"label": "Japan",         "continent": "Asia"},
    "AF":    {"label": "Africa",        "continent": "Africa"},
    "Other": {"label": "Rest of World", "continent": "Various"},
}

# Colonnes de ventes présentes dans le dataset
SALES_COLS = {
    "na_sales":    "NA",
    "pal_sales":   "EU",
    "jp_sales":    "JP",
    "other_sales": "Other",
}

# ── Étape 1 : Chargement du CSV Kaggle ─────────────────────
def load_kaggle_csv(filepath: str) -> pd.DataFrame:
    """Charge le CSV téléchargé depuis Kaggle."""
    log.info(f"Chargement du fichier : {filepath}")
    df = pd.read_csv(filepath, low_memory=False)
    log.info(f"  → {len(df):,} lignes, {df.shape[1]} colonnes")
    return df


# ── Étape 2 : Nettoyage & normalisation ────────────────────
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie et normalise le DataFrame brut."""
    log.info("Nettoyage du DataFrame...")

    # Normalise les noms de colonnes
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # Supprime les doublons évidents
    before = len(df)
    df = df.drop_duplicates(subset=["title", "release_date"], keep="first")
    log.info(f"  Doublons supprimés : {before - len(df)}")

    # Colonnes numériques : forcer float
    num_cols = list(SALES_COLS.keys()) + ["total_sales", "critic_score", "user_score"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Année d'extraction depuis release_date
    if "release_date" in df.columns:
        df["release_year"] = pd.to_datetime(
            df["release_date"], errors="coerce"
        ).dt.year.astype("Int64")

    # Supprime les lignes sans titre ni plateforme
    df = df.dropna(subset=["title"])
    df["platform"] = df.get("console", df.get("platform", "Unknown")).fillna("Unknown")
    df["genre"]    = df.get("genre", pd.Series("Unknown", index=df.index)).fillna("Unknown")

    log.info(f"  Lignes après nettoyage : {len(df):,}")
    return df


# ── Étape 3 : Pivot ventes par région ──────────────────────
def pivot_sales_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforme les colonnes de ventes (na_sales, pal_sales…)
    en une table longue : une ligne par (jeu × région).
    """
    log.info("Pivot ventes → format long par région...")

    # Colonnes disponibles dans ce dataset
    available_sales = {k: v for k, v in SALES_COLS.items() if k in df.columns}

    id_cols = ["title", "platform", "genre", "publisher", "developer",
               "release_year", "critic_score", "user_score"]
    id_cols = [c for c in id_cols if c in df.columns]

    records = []
    for _, row in df.iterrows():
        for col, region_code in available_sales.items():
            val = row.get(col)
            if pd.notna(val) and val > 0:
                meta = REGION_CONTINENT_MAP.get(region_code, {})
                records.append({
                    **{c: row[c] for c in id_cols},
                    "region_code": region_code,
                    "region_label": meta.get("label", region_code),
                    "continent":    meta.get("continent", "Unknown"),
                    "sales_millions": round(float(val), 4),
                })

    result = pd.DataFrame(records)
    log.info(f"  Lignes après pivot : {len(result):,}")
    return result


# ── Étape 4 : Enrichissement RAWG API ──────────────────────
def enrich_with_rawg(titles: list[str], max_games: int = 500) -> pd.DataFrame:
    """
    Récupère des métadonnées complémentaires depuis l'API RAWG.
    - genres détaillés, tags, rating ESRB, site officiel…
    """
    base_url = "https://api.rawg.io/api/games"
    results  = []
    titles   = titles[:max_games]

    log.info(f"Enrichissement RAWG pour {len(titles)} jeux...")

    for i, title in enumerate(titles, 1):
        try:
            resp = requests.get(base_url, params={
                "key":    RAWG_API_KEY,
                "search": title,
                "page_size": 1,
            }, timeout=10)

            if resp.status_code == 200:
                data = resp.json().get("results", [])
                if data:
                    g = data[0]
                    results.append({
                        "title":          title,
                        "rawg_id":        g.get("id"),
                        "rawg_rating":    g.get("rating"),
                        "rawg_genres":    ", ".join(x["name"] for x in g.get("genres", [])),
                        "rawg_tags":      ", ".join(x["name"] for x in g.get("tags", [])[:5]),
                        "esrb_rating":    (g.get("esrb_rating") or {}).get("name"),
                        "background_img": g.get("background_image"),
                    })

            # Respect rate-limit RAWG (5 req/s max)
            if i % 5 == 0:
                time.sleep(1)
                log.info(f"  RAWG : {i}/{len(titles)} traités")

        except requests.RequestException as e:
            log.warning(f"  RAWG erreur pour '{title}': {e}")

    df_rawg = pd.DataFrame(results)
    log.info(f"  RAWG enrichissement terminé : {len(df_rawg)} jeux enrichis")
    return df_rawg


# ── Étape 5 : Sauvegarde Parquet (partitionné) ─────────────
def save_partitioned_parquet(df: pd.DataFrame, output_dir: Path):
    """
    Sauvegarde en Parquet partitionné par continent/année.
    Simule un Data Lake (structure compatible S3/MinIO/HDFS).
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    for (continent, year), group in df.groupby(["continent", "release_year"]):
        if pd.isna(year):
            year = "unknown"
        part_dir = output_dir / f"continent={continent}" / f"year={int(year)}"
        part_dir.mkdir(parents=True, exist_ok=True)
        filepath = part_dir / "data.parquet"
        group.to_parquet(filepath, index=False, engine="pyarrow")

    log.info(f"Parquet partitionné sauvegardé dans : {output_dir}")


# ── Étape 6 : Statistiques de base ─────────────────────────
def print_summary(df: pd.DataFrame):
    """Affiche un résumé rapide des données ingérées."""
    print("\n" + "="*60)
    print("  RÉSUMÉ DU DATASET")
    print("="*60)
    print(f"  Lignes totales        : {len(df):>10,}")
    print(f"  Jeux uniques          : {df['title'].nunique():>10,}")
    print(f"  Plateformes           : {df['platform'].nunique():>10,}")
    print(f"  Genres                : {df['genre'].nunique():>10,}")
    print(f"  Continents            : {df['continent'].nunique():>10,}")
    print(f"  Années (min/max)      : {df['release_year'].min()} → {df['release_year'].max()}")
    print(f"\n  Ventes par continent (millions) :")
    sales_by_cont = (
        df.groupby("continent")["sales_millions"]
        .sum()
        .sort_values(ascending=False)
    )
    for cont, val in sales_by_cont.items():
        print(f"    {cont:<20} {val:>10,.1f} M")
    print("="*60 + "\n")


# ── Pipeline principal ──────────────────────────────────────
def run_ingestion(kaggle_csv_path: str, enrich_rawg: bool = False):
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    STAGED_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Chargement
    df_raw = load_kaggle_csv(kaggle_csv_path)
    df_raw.to_parquet(RAW_DIR / "vgsales_raw.parquet", index=False)

    # 2. Nettoyage
    df_clean = clean_dataframe(df_raw)

    # 3. Pivot régions
    df_long = pivot_sales_by_region(df_clean)

    # 4. Enrichissement RAWG (optionnel)
    if enrich_rawg and RAWG_API_KEY != "YOUR_RAWG_API_KEY":
        top_titles = df_clean["title"].dropna().unique().tolist()
        df_rawg = enrich_with_rawg(top_titles, max_games=200)
        df_long = df_long.merge(df_rawg, on="title", how="left")

    # 5. Sauvegarde
    df_long.to_parquet(STAGED_DIR / "vgsales_staged.parquet", index=False)
    save_partitioned_parquet(df_long, STAGED_DIR / "partitioned")

    # 6. Résumé
    print_summary(df_long)

    log.info("✅ Ingestion terminée avec succès !")
    return df_long


if __name__ == "__main__":
    import sys
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/vgsales.csv"
    run_ingestion(csv_path, enrich_rawg=False)
