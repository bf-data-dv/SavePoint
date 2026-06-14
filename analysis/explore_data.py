"""
=============================================================
 Exploration et Visualisation des données enrichies
 Fichier : analysis/explore_data.py
 
 Ce script réalise l'analyse descriptive (EDA - Exploratory Data Analysis)
 et la visualisation des jeux enrichis :
   1. Chargement du dataset via Parquet (optimisation des I/O).
   2. Calcul des métriques de qualité (Match Rate).
   3. Génération de visualisations pour le reporting.
=============================================================
"""

# 1. IMPORTATIONS : Bibliothèques standard et tierces
# Pandas pour la manipulation de données (DataFrames)
# Matplotlib pour le rendu visuel
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# 2. CONFIGURATION : Gestion robuste des chemins
# Utilisation de Path pour assurer la compatibilité entre Windows/Linux/macOS
DATA_PATH = Path(__file__).parent.parent / "data" / "staged" / "vgsales_enriched.parquet"

# 3. FONCTIONS MÉTIER
def plot_data_source(df: pd.DataFrame):
    """
    Visualisation : Affiche la répartition des jeux par source.
    Cette fonction permet de quantifier visuellement l'apport de l'API RAWG.
    """
    # Calcul des occurrences par source
    source_counts = df["data_source"].value_counts()
    
    # Configuration du canvas de la figure
    plt.figure(figsize=(8, 5))
    
    # Rendu graphique : Barre simple avec personnalisation
    source_counts.plot(kind='bar', color=['#3498db', '#e74c3c'], edgecolor='black')
    
    # Personnalisation des axes et titres pour un rendu "pro"
    plt.title("Répartition des sources de données (vgchartz vs enrichi)", fontsize=14)
    plt.ylabel("Nombre de jeux", fontsize=12)
    plt.xticks(rotation=0)
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    
    # Ajustement automatique de la mise en page et affichage
    plt.tight_layout()
    plt.show()

def analyze_dataset(path: Path):
    """
    Analyse descriptive : Calcule le taux d'enrichissement et affiche 
    des statistiques clés dans le terminal.
    """
    if not path.exists():
        print(f"Erreur fatale : Le fichier {path} est introuvable.")
        return None

    # Chargement du fichier Parquet (format colonnaire très rapide)
    df = pd.read_parquet(path)
    
    # Calcul des indicateurs de performance (KPIs)
    total = len(df)
    success_count = df["data_source"].str.contains("rawg").sum()
    match_rate = (success_count / total) * 100
    
    # Rapport textuel
    print(f"--- Rapport d'analyse de la Data Pipeline ---")
    print(f"Nombre total de jeux traités : {total:,}")
    print(f"Jeux enrichis avec succès :   {success_count:,}")
    print(f"Taux de correspondance :      {match_rate:.2f}%")
    print(f"---------------------------------------------")
    
    return df

# 4. BLOC D'EXÉCUTION (Entry Point)
# Ce bloc permet de n'exécuter le code que si le fichier est lancé directement.
if __name__ == "__main__":
    df = analyze_dataset(DATA_PATH)
    
    if df is not None:
        # Appel de la fonction de visualisation après analyse réussie
        plot_data_source(df)