"""
=============================================================
 SavePoint — Enrichissement RAWG API (Version Robuste)
 Fichier : ingestion/enrichment_rawg.py
 
 Ce script est un pipeline ETL (Extract, Transform, Load) :
   1. E : Extraction des données depuis un fichier CSV source.
   2. T : Transformation (nettoyage) et Enrichissement (API externe).
   3. L : Chargement des données enrichies dans un fichier Parquet.
=============================================================
"""

import os
import time
import logging
import requests
import re
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ── 1. CONFIGURATION (Paramétrage de l'environnement) ────────
# Le chargement des variables d'environnement permet de ne jamais coder
# en dur des informations sensibles comme les clés API.
load_dotenv()
RAWG_API_KEY  = os.getenv("RAWG_API_KEY")
RAWG_BASE_URL = "https://api.rawg.io/api/games"

# Définition des chemins via Pathlib : meilleure gestion multi-OS
CSV_PATH      = Path("data/raw/vgchartz-2024.csv")
STAGED_DIR    = Path("data/staged")
OUTPUT_PATH   = STAGED_DIR / "vgsales_enriched.parquet"

# Constantes de contrôle pour le "Rate Limiting" (protection contre le bannissement API)
MAX_GAMES     = 1000
SLEEP_EVERY   = 5
SLEEP_SECONDS = 1.0

# Logging : remplace les simples 'print' pour tracer l'exécution dans les logs système
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── 2. FONCTIONS MÉTIER (Logique de transformation) ─────────

def clean_title(title: str) -> str:
    """
    Normalisation : Cette étape est cruciale pour le 'Data Matching'.
    On élimine le bruit (parenthèses, ponctuations) pour que le moteur 
    de recherche de l'API puisse trouver le jeu sans erreur de syntaxe.
    """
    title = re.sub(r'\s*\([^)]*\)', '', str(title))
    title = re.sub(r'[^a-zA-Z0-9\s]', '', title)
    return title.strip()

def load_csv(path: Path) -> pd.DataFrame:
    """Chargement : Lecture du dataset brut avec optimisations de mémoire."""
    return pd.read_csv(path, low_memory=False)

def find_games_to_enrich(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtrage : Identification des données 'sales' (manquantes).
    On déduplique sur le titre pour ne pas interroger l'API 10 fois pour le même jeu.
    """
    mask = (df["critic_score"].isna() | df["developer"].isna() | df["release_date"].isna())
    return df[mask].drop_duplicates(subset=["title"]).head(MAX_GAMES)

def fetch_rawg(title: str) -> dict | None:
    """
    Appel API : Logique de requête HTTP avec gestion des erreurs réseau.
    Le filtrage ici permet de s'assurer que la réponse API correspond bien 
    au jeu recherché (vérification de nom).
    """
    clean_t = clean_title(title)
    try:
        resp = requests.get(
            RAWG_BASE_URL,
            params={"key": RAWG_API_KEY, "search": clean_t, "page_size": 5},
            timeout=10,
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            for game in results:
                # Filtrage métier : on valide que le nom renvoyé contient le titre cherché
                if title.lower() in game.get("name", "").lower():
                    return game
            return results[0] if results else None
    except requests.RequestException as e:
        log.warning(f"Erreur réseau pour '{title}': {e}")
    return None

def parse_rawg_result(game: dict) -> dict:
    """Projection : On ne garde que les colonnes nécessaires (Data Minimization)."""
    return {
        "rawg_id":        game.get("id"),
        "rawg_released":  game.get("released"),
        "rawg_metacritic":game.get("metacritic"),
        "rawg_developers": ", ".join([d["name"] for d in game.get("developers", [])]) if game.get("developers") else None,
    }

def enrich_dataframe(df: pd.DataFrame, games_to_enrich: pd.DataFrame) -> pd.DataFrame:
    """
    Moteur principal : Orchestre l'appel API, le Checkpointing et la Fusion.
    """
    enrichment_map = {}
    
    # CHECKPOINTING : Reprise du travail en cas de plantage.
    # On lit le fichier final déjà généré pour ne pas refaire les appels API réussis.
    if OUTPUT_PATH.exists():
        log.info("Checkpoint détecté : Chargement des succès précédents.")
        df_existing = pd.read_parquet(OUTPUT_PATH)
        for _, row in df_existing[df_existing["rawg_id"].notna()].iterrows():
            enrichment_map[row["title"]] = {
                "rawg_id": row["rawg_id"],
                "rawg_released": row["rawg_released"],
                "rawg_metacritic": row["rawg_metacritic"],
                "rawg_developers": row["rawg_developers"]
            }

    total = len(games_to_enrich)
    for i, (_, row) in enumerate(games_to_enrich.iterrows(), 1):
        title = row["title"]
        if title in enrichment_map: continue # Le jeu est déjà enrichi, on passe.
            
        log.info(f" [{i}/{total}] Recherche : {title}")
        result = fetch_rawg(title)
        if result:
            enrichment_map[title] = parse_rawg_result(result)
        
        # Gestion du quota API : pause temporelle entre les lots.
        if i % SLEEP_EVERY == 0: time.sleep(SLEEP_SECONDS)

    # FUSION : Jointure entre le CSV source et les nouvelles données API.
    df_rawg = pd.DataFrame.from_dict(enrichment_map, orient="index").reset_index().rename(columns={"index": "title"})
    df_enriched = df.merge(df_rawg, on="title", how="left")
    
    # Traçabilité : on marque l'origine de la ligne (source vgchartz vs enrichie).
    df_enriched["data_source"] = "vgchartz"
    df_enriched.loc[df_enriched["rawg_id"].notna(), "data_source"] = "vgchartz+rawg"
    return df_enriched


# ── 3. PIPELINE (Exécution) ──────────────────────────────────

def run():
    # Création automatique du répertoire de travail si nécessaire.
    STAGED_DIR.mkdir(parents=True, exist_ok=True)
    
    # Pipeline séquentiel.
    df = load_csv(CSV_PATH)
    games_to_enrich = find_games_to_enrich(df)
    df_enriched = enrich_dataframe(df, games_to_enrich)
    
    # Sauvegarde : Format Parquet privilégié pour sa compression et rapidité de lecture.
    df_enriched.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow")
    log.info(f"✅ Pipeline terminé avec succès : {OUTPUT_PATH}")

if __name__ == "__main__":
    run()