-- =============================================================
--  SavePoint — Schéma Data Warehouse Snowflake
--  Exécute ce script dans Snowflake Worksheets
-- =============================================================

-- ── Setup initial ────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS SAVEPOINT;
USE DATABASE SAVEPOINT;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- Warehouse de calcul (utilise le défaut si déjà existant)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'SavePoint pipeline warehouse';

-- ── Stage interne (pour charger les Parquet) ─────────────────
CREATE STAGE IF NOT EXISTS savepoint_stage
    FILE_FORMAT = (TYPE = 'PARQUET')
    COMMENT     = 'Stage pour upload des fichiers Parquet';

-- =============================================================
--  DIMENSIONS
-- =============================================================

CREATE TABLE IF NOT EXISTS DIM_GAME (
    GAME_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    TITLE           VARCHAR(300)   NOT NULL,
    PUBLISHER       VARCHAR(200),
    DEVELOPER       VARCHAR(200),
    ESRB_RATING     VARCHAR(10),
    CRITIC_SCORE    FLOAT,
    USER_SCORE      FLOAT,
    RAWG_ID         NUMBER,
    RAWG_RATING     FLOAT,
    RAWG_TAGS       TEXT,
    BACKGROUND_IMG  TEXT,
    DATA_SOURCE     VARCHAR(20)  DEFAULT 'vgchartz',
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DIM_PLATFORM (
    PLATFORM_ID     NUMBER AUTOINCREMENT PRIMARY KEY,
    PLATFORM_CODE   VARCHAR(20)  NOT NULL UNIQUE,
    PLATFORM_NAME   VARCHAR(100),
    MANUFACTURER    VARCHAR(100),
    PLATFORM_TYPE   VARCHAR(20),
    GENERATION      NUMBER,
    LAUNCH_YEAR     NUMBER
);

INSERT INTO DIM_PLATFORM (PLATFORM_CODE, PLATFORM_NAME, MANUFACTURER, PLATFORM_TYPE, GENERATION, LAUNCH_YEAR)
SELECT * FROM VALUES
    ('PS5',  'PlayStation 5',    'Sony',      'console',  9, 2020),
    ('PS4',  'PlayStation 4',    'Sony',      'console',  8, 2013),
    ('PS3',  'PlayStation 3',    'Sony',      'console',  7, 2006),
    ('PS2',  'PlayStation 2',    'Sony',      'console',  6, 2000),
    ('PS',   'PlayStation',      'Sony',      'console',  5, 1994),
    ('XSX',  'Xbox Series X/S',  'Microsoft', 'console',  9, 2020),
    ('XOne', 'Xbox One',         'Microsoft', 'console',  8, 2013),
    ('X360', 'Xbox 360',         'Microsoft', 'console',  7, 2005),
    ('XB',   'Xbox',             'Microsoft', 'console',  6, 2001),
    ('NS',   'Nintendo Switch',  'Nintendo',  'handheld', 9, 2017),
    ('WiiU', 'Wii U',            'Nintendo',  'console',  8, 2012),
    ('Wii',  'Wii',              'Nintendo',  'console',  7, 2006),
    ('GC',   'GameCube',         'Nintendo',  'console',  6, 2001),
    ('N64',  'Nintendo 64',      'Nintendo',  'console',  5, 1996),
    ('3DS',  'Nintendo 3DS',     'Nintendo',  'handheld', 8, 2011),
    ('DS',   'Nintendo DS',      'Nintendo',  'handheld', 7, 2004),
    ('GBA',  'Game Boy Advance', 'Nintendo',  'handheld', 6, 2001),
    ('PC',   'PC / Windows',     'N/A',       'pc',       0, NULL),
    ('iOS',  'iOS / iPhone',     'Apple',     'mobile',   0, NULL),
    ('And',  'Android',          'Google',    'mobile',   0, NULL)
AS t(PLATFORM_CODE, PLATFORM_NAME, MANUFACTURER, PLATFORM_TYPE, GENERATION, LAUNCH_YEAR);

CREATE TABLE IF NOT EXISTS DIM_GENRE (
    GENRE_ID    NUMBER AUTOINCREMENT PRIMARY KEY,
    GENRE_NAME  VARCHAR(100) NOT NULL UNIQUE,
    CATEGORY    VARCHAR(50)
);

INSERT INTO DIM_GENRE (GENRE_NAME, CATEGORY)
SELECT * FROM VALUES
    ('Action',             'Action'),
    ('Action-Adventure',   'Action'),
    ('Adventure',          'Adventure'),
    ('Fighting',           'Action'),
    ('Shooter',            'Action'),
    ('Platform',           'Action'),
    ('Role-Playing',       'RPG'),
    ('JRPG',               'RPG'),
    ('MMO',                'RPG'),
    ('Strategy',           'Strategy'),
    ('Simulation',         'Simulation'),
    ('Sports',             'Sports'),
    ('Racing',             'Sports'),
    ('Puzzle',             'Puzzle'),
    ('Music',              'Casual'),
    ('Party',              'Casual'),
    ('Sandbox',            'Casual'),
    ('Visual Novel',       'Other'),
    ('Board Game',         'Other'),
    ('Education',          'Other'),
    ('Misc',               'Other')
AS t(GENRE_NAME, CATEGORY);

CREATE TABLE IF NOT EXISTS DIM_REGION (
    REGION_ID       NUMBER AUTOINCREMENT PRIMARY KEY,
    REGION_CODE     VARCHAR(10)  NOT NULL UNIQUE,
    REGION_LABEL    VARCHAR(100),
    CONTINENT       VARCHAR(50),
    CONTINENT_CODE  CHAR(2)
);

INSERT INTO DIM_REGION (REGION_CODE, REGION_LABEL, CONTINENT, CONTINENT_CODE)
SELECT * FROM VALUES
    ('NA',    'North America',  'Americas', 'AM'),
    ('EU',    'Europe',         'Europe',   'EU'),
    ('JP',    'Japan',          'Asia',     'AS'),
    ('AF',    'Africa',         'Africa',   'AF'),
    ('Other', 'Rest of World',  'Various',  'XX')
AS t(REGION_CODE, REGION_LABEL, CONTINENT, CONTINENT_CODE);

-- =============================================================
--  TABLE DE FAITS
-- =============================================================

CREATE TABLE IF NOT EXISTS FACT_SALES (
    SALE_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    GAME_ID         NUMBER         NOT NULL REFERENCES DIM_GAME(GAME_ID),
    PLATFORM_ID     NUMBER         REFERENCES DIM_PLATFORM(PLATFORM_ID),
    GENRE_ID        NUMBER         REFERENCES DIM_GENRE(GENRE_ID),
    REGION_ID       NUMBER         NOT NULL REFERENCES DIM_REGION(REGION_ID),
    RELEASE_YEAR    NUMBER,
    SALES_MILLIONS  FLOAT,
    GLOBAL_SALES    FLOAT,
    INGESTED_AT     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================
--  VUES ANALYTIQUES
-- =============================================================

CREATE OR REPLACE VIEW VW_SALES_CONTINENT_GENRE AS
SELECT
    dr.CONTINENT,
    dg.GENRE_NAME,
    fs.RELEASE_YEAR,
    COUNT(DISTINCT fs.GAME_ID)      AS NB_GAMES,
    SUM(fs.SALES_MILLIONS)          AS TOTAL_SALES_M,
    AVG(dga.CRITIC_SCORE)           AS AVG_CRITIC_SCORE
FROM FACT_SALES fs
JOIN DIM_REGION  dr  ON fs.REGION_ID  = dr.REGION_ID
JOIN DIM_GENRE   dg  ON fs.GENRE_ID   = dg.GENRE_ID
JOIN DIM_GAME    dga ON fs.GAME_ID    = dga.GAME_ID
GROUP BY dr.CONTINENT, dg.GENRE_NAME, fs.RELEASE_YEAR;

CREATE OR REPLACE VIEW VW_TOP_PLATFORMS_BY_REGION AS
SELECT
    dr.REGION_LABEL,
    dr.CONTINENT,
    dp.PLATFORM_NAME,
    dp.MANUFACTURER,
    dp.GENERATION,
    SUM(fs.SALES_MILLIONS)          AS TOTAL_SALES_M,
    COUNT(DISTINCT fs.GAME_ID)      AS NB_TITLES
FROM FACT_SALES fs
JOIN DIM_REGION   dr ON fs.REGION_ID   = dr.REGION_ID
JOIN DIM_PLATFORM dp ON fs.PLATFORM_ID = dp.PLATFORM_ID
GROUP BY dr.REGION_LABEL, dr.CONTINENT, dp.PLATFORM_NAME, dp.MANUFACTURER, dp.GENERATION
ORDER BY TOTAL_SALES_M DESC;

CREATE OR REPLACE VIEW VW_YEARLY_GLOBAL_SALES AS
SELECT
    RELEASE_YEAR,
    COUNT(DISTINCT GAME_ID)         AS NB_GAMES_RELEASED,
    SUM(SALES_MILLIONS)             AS TOTAL_SALES_M,
    AVG(SALES_MILLIONS)             AS AVG_SALES_PER_GAME_M,
    MAX(SALES_MILLIONS)             AS MAX_SALES_M
FROM FACT_SALES
WHERE RELEASE_YEAR IS NOT NULL
GROUP BY RELEASE_YEAR
ORDER BY RELEASE_YEAR;

CREATE OR REPLACE VIEW VW_GAME_RANKING_BY_REGION AS
SELECT
    dr.REGION_LABEL,
    dga.TITLE,
    dp.PLATFORM_NAME,
    dg.GENRE_NAME,
    fs.SALES_MILLIONS,
    RANK() OVER (
        PARTITION BY dr.REGION_ID
        ORDER BY fs.SALES_MILLIONS DESC
    ) AS REGION_RANK
FROM FACT_SALES fs
JOIN DIM_GAME     dga ON fs.GAME_ID     = dga.GAME_ID
JOIN DIM_REGION   dr  ON fs.REGION_ID   = dr.REGION_ID
JOIN DIM_PLATFORM dp  ON fs.PLATFORM_ID = dp.PLATFORM_ID
JOIN DIM_GENRE    dg  ON fs.GENRE_ID    = dg.GENRE_ID;
