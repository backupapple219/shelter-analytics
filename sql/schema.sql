-- schema.sql
-- Shelter Analytics Pipeline — SQLite schema
-- Run automatically by 03_load_db.py

-- ─────────────────────────────────────────────────────
-- dim_animal_type
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_animal_type (
    animal_type_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_type     TEXT NOT NULL UNIQUE
);

-- ─────────────────────────────────────────────────────
-- dim_breed
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_breed (
    breed_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    primary_breed TEXT NOT NULL UNIQUE
);

-- ─────────────────────────────────────────────────────
-- dim_outcome_category
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_outcome_category (
    outcome_category_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    outcome_category     TEXT NOT NULL UNIQUE,
    is_live_outcome      INTEGER NOT NULL DEFAULT 0  -- 1 = live, 0 = non-live
);

-- ─────────────────────────────────────────────────────
-- fact_animal
-- Core fact table — one row per intake/outcome event pair
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_animal (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    animal_id             TEXT NOT NULL,
    name                  TEXT,
    has_name              INTEGER,          -- 1 / 0

    -- Intake
    intake_date           TEXT,
    intake_year           INTEGER,
    intake_month          INTEGER,
    intake_season         TEXT,
    intake_dow            TEXT,
    intake_type           TEXT,
    intake_condition      TEXT,
    found_location        TEXT,

    -- Dimensions (FK-style text keys; SQLite doesn't enforce FK by default)
    animal_type           TEXT,
    primary_breed         TEXT,
    is_mix                INTEGER,          -- 1 / 0
    color                 TEXT,
    sex                   TEXT,
    neuter_status_intake  TEXT,
    age_group             TEXT,
    age_years_intake      REAL,
    age_days_intake       REAL,

    -- Outcome
    outcome_date          TEXT,
    outcome_year          INTEGER,
    outcome_month         INTEGER,
    outcome_season        TEXT,
    outcome_dow           TEXT,
    outcome_type          TEXT,
    outcome_subtype       TEXT,
    outcome_category      TEXT,
    is_live_outcome       INTEGER,          -- 1 / 0
    neuter_status_outcome TEXT,

    -- Derived
    length_of_stay_days   INTEGER
);

-- ─────────────────────────────────────────────────────
-- agg_monthly_intake
-- Pre-aggregated for fast dashboard queries
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_monthly_intake (
    year          INTEGER,
    month         INTEGER,
    animal_type   TEXT,
    intake_count  INTEGER,
    PRIMARY KEY (year, month, animal_type)
);

-- ─────────────────────────────────────────────────────
-- agg_outcome_summary
-- ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS agg_outcome_summary (
    animal_type       TEXT,
    outcome_category  TEXT,
    outcome_count     INTEGER,
    avg_los_days      REAL,
    PRIMARY KEY (animal_type, outcome_category)
);
