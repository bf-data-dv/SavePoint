-- =============================================================
--  VIDEO GAME SALES — Schéma Data Warehouse (étoile)
--  Compatible : PostgreSQL 14+ / BigQuery / Snowflake
-- =============================================================

-- ─────────────────────────────────────────────────────────────
--  DIMENSIONS
-- ─────────────────────────────────────────────────────────────

-- dim_game : informations sur chaque jeu
CREATE TABLE IF NOT EXISTS dim_game (
    game_id         SERIAL PRIMARY KEY,
    title           VARCHAR(300)    NOT NULL,
    publisher       VARCHAR(200),
    developer       VARCHAR(200),
    esrb_rating     VARCHAR(10),        -- E, T, M, AO, RP
    critic_score    NUMERIC(4,1),       -- /10
    user_score      NUMERIC(4,1),       -- /10
    rawg_id         INTEGER,
    rawg_rating     NUMERIC(3,2),       -- /5
    rawg_tags       TEXT,
    background_img  TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (title, publisher)
);

-- dim_platform : consoles et plateformes
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_id     SERIAL PRIMARY KEY,
    platform_code   VARCHAR(20)  NOT NULL UNIQUE,   -- PS5, XSX, NS, PC…
    platform_name   VARCHAR(100),
    manufacturer    VARCHAR(100),                    -- Sony, Microsoft, Nintendo…
    platform_type   VARCHAR(20)
        CHECK (platform_type IN ('console', 'handheld', 'pc', 'mobile', 'other')),
    generation      SMALLINT,                        -- 7, 8, 9…
    launch_year     SMALLINT
);

-- Données de référence plateformes
INSERT INTO dim_platform (platform_code, platform_name, manufacturer, platform_type, generation, launch_year)
VALUES
    ('PS5',  'PlayStation 5',       'Sony',       'console',  9, 2020),
    ('PS4',  'PlayStation 4',       'Sony',       'console',  8, 2013),
    ('PS3',  'PlayStation 3',       'Sony',       'console',  7, 2006),
    ('PS2',  'PlayStation 2',       'Sony',       'console',  6, 2000),
    ('PS',   'PlayStation',         'Sony',       'console',  5, 1994),
    ('XSX',  'Xbox Series X/S',     'Microsoft',  'console',  9, 2020),
    ('XOne', 'Xbox One',            'Microsoft',  'console',  8, 2013),
    ('X360', 'Xbox 360',            'Microsoft',  'console',  7, 2005),
    ('XB',   'Xbox',                'Microsoft',  'console',  6, 2001),
    ('NS',   'Nintendo Switch',     'Nintendo',   'handheld', 9, 2017),
    ('WiiU', 'Wii U',               'Nintendo',   'console',  8, 2012),
    ('Wii',  'Wii',                 'Nintendo',   'console',  7, 2006),
    ('GC',   'GameCube',            'Nintendo',   'console',  6, 2001),
    ('N64',  'Nintendo 64',         'Nintendo',   'console',  5, 1996),
    ('3DS',  'Nintendo 3DS',        'Nintendo',   'handheld', 8, 2011),
    ('DS',   'Nintendo DS',         'Nintendo',   'handheld', 7, 2004),
    ('GBA',  'Game Boy Advance',    'Nintendo',   'handheld', 6, 2001),
    ('PC',   'PC / Windows',        'N/A',        'pc',       0, NULL),
    ('iOS',  'iOS / iPhone',        'Apple',      'mobile',   0, NULL),
    ('And',  'Android',             'Google',     'mobile',   0, NULL)
ON CONFLICT (platform_code) DO NOTHING;

-- dim_genre : genres de jeux
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id    SERIAL PRIMARY KEY,
    genre_name  VARCHAR(100) NOT NULL UNIQUE,
    category    VARCHAR(50)             -- Action, Simulation, Strategy, RPG…
);

INSERT INTO dim_genre (genre_name, category)
VALUES
    ('Action',          'Action'),
    ('Action-Adventure','Action'),
    ('Adventure',       'Adventure'),
    ('Fighting',        'Action'),
    ('Shooter',         'Action'),
    ('Platform',        'Action'),
    ('RPG',             'RPG'),
    ('JRPG',            'RPG'),
    ('MMORPG',          'RPG'),
    ('Strategy',        'Strategy'),
    ('Turn-Based Strategy','Strategy'),
    ('Real-Time Strategy','Strategy'),
    ('Simulation',      'Simulation'),
    ('Sports',          'Sports'),
    ('Racing',          'Sports'),
    ('Puzzle',          'Puzzle'),
    ('Music',           'Casual'),
    ('Party',           'Casual'),
    ('Misc',            'Other')
ON CONFLICT (genre_name) DO NOTHING;

-- dim_region : régions et continents
CREATE TABLE IF NOT EXISTS dim_region (
    region_id       SERIAL PRIMARY KEY,
    region_code     VARCHAR(10)  NOT NULL UNIQUE,
    region_label    VARCHAR(100),
    continent       VARCHAR(50),
    continent_code  CHAR(2)
);

INSERT INTO dim_region (region_code, region_label, continent, continent_code)
VALUES
    ('NA',    'North America',   'Americas', 'AM'),
    ('EU',    'Europe',          'Europe',   'EU'),
    ('JP',    'Japan',           'Asia',     'AS'),
    ('AF',    'Africa',          'Africa',   'AF'),
    ('Other', 'Rest of World',   'Various',  'XX')
ON CONFLICT (region_code) DO NOTHING;

-- dim_date : calendrier pour analyses temporelles
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     INTEGER PRIMARY KEY,   -- format YYYYMMDD
    full_date   DATE,
    year        SMALLINT,
    quarter     SMALLINT,
    month       SMALLINT,
    month_name  VARCHAR(20),
    week        SMALLINT,
    day_of_week SMALLINT,
    is_weekend  BOOLEAN
);

-- Génération du calendrier 1970–2030
INSERT INTO dim_date
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(YEAR    FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH   FROM d)::SMALLINT,
    TO_CHAR(d, 'Month'),
    EXTRACT(WEEK    FROM d)::SMALLINT,
    EXTRACT(DOW     FROM d)::SMALLINT,
    EXTRACT(DOW     FROM d) IN (0, 6)
FROM generate_series('1970-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS d
ON CONFLICT (date_id) DO NOTHING;

-- ─────────────────────────────────────────────────────────────
--  TABLE DE FAITS
-- ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         BIGSERIAL PRIMARY KEY,
    game_id         INTEGER     NOT NULL REFERENCES dim_game(game_id),
    platform_id     INTEGER     REFERENCES dim_platform(platform_id),
    genre_id        INTEGER     REFERENCES dim_genre(genre_id),
    region_id       INTEGER     NOT NULL REFERENCES dim_region(region_id),
    release_year    SMALLINT,
    sales_millions  NUMERIC(10, 4),     -- ventes en millions d'unités
    global_sales    NUMERIC(10, 4),     -- total mondial (dénormalisé pour perf)
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────
--  INDEX (performances analytiques)
-- ─────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_fact_sales_game        ON fact_sales(game_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_platform    ON fact_sales(platform_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_genre       ON fact_sales(genre_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_region      ON fact_sales(region_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_year        ON fact_sales(release_year);
CREATE INDEX IF NOT EXISTS idx_dim_game_title         ON dim_game(title);

-- ─────────────────────────────────────────────────────────────
--  VUES ANALYTIQUES
-- ─────────────────────────────────────────────────────────────

-- Vue 1 : Ventes par continent et genre
CREATE OR REPLACE VIEW vw_sales_continent_genre AS
SELECT
    dr.continent,
    dg.genre_name,
    fs.release_year,
    COUNT(DISTINCT fs.game_id)          AS nb_games,
    SUM(fs.sales_millions)              AS total_sales_M,
    AVG(dga.critic_score)               AS avg_critic_score
FROM fact_sales fs
JOIN dim_region   dr  ON fs.region_id   = dr.region_id
JOIN dim_genre    dg  ON fs.genre_id    = dg.genre_id
JOIN dim_game     dga ON fs.game_id     = dga.game_id
GROUP BY dr.continent, dg.genre_name, fs.release_year;

-- Vue 2 : Top plateformes par région
CREATE OR REPLACE VIEW vw_top_platforms_by_region AS
SELECT
    dr.region_label,
    dr.continent,
    dp.platform_name,
    dp.manufacturer,
    dp.generation,
    SUM(fs.sales_millions)  AS total_sales_M,
    COUNT(DISTINCT fs.game_id) AS nb_titles
FROM fact_sales fs
JOIN dim_region   dr ON fs.region_id   = dr.region_id
JOIN dim_platform dp ON fs.platform_id = dp.platform_id
GROUP BY dr.region_label, dr.continent, dp.platform_name, dp.manufacturer, dp.generation
ORDER BY total_sales_M DESC;

-- Vue 3 : Évolution des ventes mondiales par année
CREATE OR REPLACE VIEW vw_yearly_global_sales AS
SELECT
    fs.release_year,
    COUNT(DISTINCT fs.game_id)   AS nb_games_released,
    SUM(fs.sales_millions)       AS total_sales_M,
    AVG(fs.sales_millions)       AS avg_sales_per_game_M,
    MAX(fs.sales_millions)       AS max_sales_M
FROM fact_sales fs
WHERE fs.release_year IS NOT NULL
GROUP BY fs.release_year
ORDER BY fs.release_year;

-- Vue 4 : Classement des jeux par région
CREATE OR REPLACE VIEW vw_game_ranking_by_region AS
SELECT
    dr.region_label,
    dga.title,
    dp.platform_name,
    dg.genre_name,
    fs.sales_millions,
    RANK() OVER (
        PARTITION BY dr.region_id
        ORDER BY fs.sales_millions DESC
    ) AS region_rank
FROM fact_sales fs
JOIN dim_game     dga ON fs.game_id     = dga.game_id
JOIN dim_region   dr  ON fs.region_id   = dr.region_id
JOIN dim_platform dp  ON fs.platform_id = dp.platform_id
JOIN dim_genre    dg  ON fs.genre_id    = dg.genre_id;
