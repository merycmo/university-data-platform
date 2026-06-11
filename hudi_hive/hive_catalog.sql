-- hudi_hive/hive_catalog.sql

-- ─────────────────────────────────────────
-- Créer la base de données
-- ─────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS university_db
COMMENT 'Base de données université Hassan II et Cadi Ayyad'
LOCATION 's3a://curated/';

USE university_db;

-- ─────────────────────────────────────────
-- Table faculty_profiles
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS faculty_profiles (
    record_id          STRING,
    name               STRING,
    orcid              STRING,
    publications_count BIGINT,
    citations_count    BIGINT,
    university         STRING,
    faculty            STRING,
    source             STRING,
    crawl_timestamp    TIMESTAMP
)
PARTITIONED BY (university STRING)
STORED AS PARQUET
LOCATION 's3a://curated/faculty_profiles/';

-- ─────────────────────────────────────────
-- Table course_catalog
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS course_catalog (
    record_id       STRING,
    title           STRING,
    level           STRING,
    department      STRING,
    language        STRING,
    university      STRING,
    faculty         STRING,
    source          STRING,
    crawl_timestamp TIMESTAMP
)
PARTITIONED BY (university STRING)
STORED AS PARQUET
LOCATION 's3a://curated/course_catalog/';

-- ─────────────────────────────────────────
-- Requêtes utiles pour Metabase
-- ─────────────────────────────────────────

-- Nombre de profs par faculté
SELECT faculty, COUNT(*) as nb_profs
FROM faculty_profiles
GROUP BY faculty
ORDER BY nb_profs DESC;

-- Nombre de cours par niveau
SELECT level, COUNT(*) as nb_cours
FROM course_catalog
GROUP BY level
ORDER BY nb_cours DESC;

-- Profs par université
SELECT university, COUNT(*) as nb_profs
FROM faculty_profiles
GROUP BY university;

-- Publications totales par université
SELECT university, SUM(publications_count) as total_publications
FROM faculty_profiles
GROUP BY university
ORDER BY total_publications DESC;