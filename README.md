# 🎮 Video Game Sales — Projet Data Engineer

Pipeline complet pour l'analyse des ventes de jeux vidéo par plateforme, genre et région mondiale.

---

## 📁 Structure du projet

```
vgsales-pipeline/
├── 01_ingestion.py       ← Script Python standalone (sans Airflow)
├── 02_schema.sql         ← Schéma Data Warehouse (PostgreSQL)
├── 03_dag_airflow.py     ← DAG Airflow (orchestration automatique)
├── data/
│   ├── raw/              ← CSV brut téléchargé depuis Kaggle
│   └── staged/           ← Parquet nettoyé + partitionné
└── README.md
```

---

## 🗃️ Sources de données

| Source | URL | Contenu |
|--------|-----|---------|
| Kaggle VGSales 2024 | https://www.kaggle.com/datasets/asaniczka/video-game-sales-2024 | 64 000 jeux, ventes NA/EU/JP/Other |
| RAWG API | https://rawg.io/apidocs | Genres, tags, ESRB, ratings |
| Maven Analytics | https://mavenanalytics.io/data-playground/video-game-sales | Même dataset, sans compte Kaggle |

---

## 🏗️ Architecture

```
Kaggle CSV ──┐
             ├─→ Ingestion Python ─→ Raw Parquet (Data Lake)
RAWG API ────┘         │
                       ▼
               Cleaning / Transform
               (normalisation, pivot régions)
                       │
                       ▼
              Staged Parquet (partitionné)
              continent= / year=
                       │
                       ▼
           PostgreSQL Data Warehouse
           ┌─────────────────────────┐
           │  dim_game               │
           │  dim_platform           │
           │  dim_genre              │
           │  dim_region             │
           │  dim_date               │
           │  fact_sales  ← centrale │
           └─────────────────────────┘
                       │
                       ▼
            Vues analytiques + Dashboard
            (Metabase / Grafana / Power BI)
```

---

## 🚀 Démarrage rapide

### 1. Prérequis

```bash
pip install pandas pyarrow requests kaggle psycopg2-binary apache-airflow
```

### 2. Télécharger les données

```bash
# Via Kaggle CLI
kaggle datasets download -d asaniczka/video-game-sales-2024 --unzip -p data/raw/

# Ou manuellement sur Maven Analytics (sans compte) :
# https://mavenanalytics.io/data-playground/video-game-sales
```

### 3. Créer le schéma PostgreSQL

```bash
psql -U postgres -d vgsales -f 02_schema.sql
```

### 4. Lancer l'ingestion standalone

```bash
python 01_ingestion.py data/raw/vgchartz-2024.csv
```

### 5. Lancer via Airflow

```bash
# Copier le DAG dans le dossier Airflow
cp 03_dag_airflow.py $AIRFLOW_HOME/dags/

# Définir les variables
airflow variables set KAGGLE_USERNAME "ton_username"
airflow variables set KAGGLE_KEY      "ta_clé_api"
airflow variables set RAWG_API_KEY    "ta_clé_rawg"

# Créer la connexion PostgreSQL
airflow connections add postgres_vgsales \
  --conn-type postgres \
  --conn-host localhost \
  --conn-schema vgsales \
  --conn-login postgres \
  --conn-password motdepasse \
  --conn-port 5432

# Déclencher le DAG
airflow dags trigger vgsales_pipeline
```

---

## 📐 Modèle de données (schéma en étoile)

```
                    dim_date
                       │
dim_genre ────┐        │
              │        ▼
dim_platform ─┼──→ fact_sales ←─── dim_region
              │     (ventes)
dim_game ─────┘
```

### Colonnes clés de `fact_sales`

| Colonne | Type | Description |
|---------|------|-------------|
| game_id | FK | Référence dim_game |
| platform_id | FK | Référence dim_platform |
| genre_id | FK | Référence dim_genre |
| region_id | FK | Référence dim_region |
| release_year | SMALLINT | Année de sortie |
| sales_millions | NUMERIC | Ventes en millions d'unités |
| global_sales | NUMERIC | Total mondial (dénormalisé) |

---

## 📊 Exemples de requêtes analytiques

### Top 10 jeux par ventes en Europe
```sql
SELECT dga.title, dp.platform_name, SUM(fs.sales_millions) AS sales_M
FROM fact_sales fs
JOIN dim_game     dga ON fs.game_id     = dga.game_id
JOIN dim_platform dp  ON fs.platform_id = dp.platform_id
JOIN dim_region   dr  ON fs.region_id   = dr.region_id
WHERE dr.region_code = 'EU'
GROUP BY dga.title, dp.platform_name
ORDER BY sales_M DESC
LIMIT 10;
```

### Ventes par genre et continent
```sql
SELECT * FROM vw_sales_continent_genre
ORDER BY total_sales_M DESC;
```

### Évolution annuelle du marché
```sql
SELECT * FROM vw_yearly_global_sales
WHERE release_year BETWEEN 2000 AND 2024;
```

### Plateformes dominantes par région
```sql
SELECT * FROM vw_top_platforms_by_region LIMIT 20;
```

---

## 🔄 Orchestration Airflow — Ordre des tâches

```
init_directories
      │
      ▼
download_kaggle      ← Kaggle CLI, hebdomadaire
      │
      ▼
clean_and_stage      ← Nettoyage + Parquet
      │
      ▼
load_dimensions      ← dim_game, dim_genre (upsert)
      │
      ▼
load_facts           ← fact_sales (pivot régions)
      │
      ▼
enrich_rawg          ← RAWG API (top 300 jeux)
      │
      ▼
data_quality_check   ← Contrôles intégrité
```

---

## 🛠️ Extensions possibles

- **dbt** : ajouter des modèles dbt pour les transformations SQL versionnées
- **MinIO / S3** : remplacer le stockage local par un vrai Data Lake cloud
- **Apache Spark** : passer à PySpark si le volume dépasse plusieurs Go
- **Great Expectations** : renforcer les contrôles qualité
- **Metabase** : connecter directement au PostgreSQL pour dashboards
- **API IGDB** (Twitch/Twitch) : enrichissement complémentaire avec covers, modes multijoueur, etc.
