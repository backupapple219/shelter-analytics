"""
03_load_db.py
-------------
Loads cleaned CSVs into a structured SQLite database.
Creates the schema (via sql/schema.sql) and populates:
  - fact_animal          (row-level merged data)
  - agg_monthly_intake   (pre-aggregated)
  - agg_outcome_summary  (pre-aggregated)
"""

import sqlite3
import pandas as pd
import os

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")
SQL_DIR       = os.path.join(os.path.dirname(__file__), "..", "sql")
DB_PATH       = os.path.join(os.path.dirname(__file__), "..", "data", "shelter.db")


def create_schema(conn: sqlite3.Connection):
    schema_path = os.path.join(SQL_DIR, "schema.sql")
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    conn.executescript(schema_sql)
    conn.commit()
    print("  Schema created / verified.")


def load_fact_animal(conn: sqlite3.Connection, df: pd.DataFrame):
    """Insert merged animal records into fact_animal."""
    fact_cols = [
        "animal_id", "name", "has_name",
        "intake_date", "intake_year", "intake_month", "intake_season",
        "intake_dow", "intake_type", "intake_condition", "found_location",
        "animal_type", "primary_breed", "is_mix", "color",
        "sex", "neuter_status_intake", "age_group",
        "age_years_intake", "age_days_intake",
        "outcome_date", "outcome_year", "outcome_month", "outcome_season",
        "outcome_dow", "outcome_type", "outcome_subtype",
        "outcome_category", "is_live_outcome", "neuter_status_outcome",
        "length_of_stay_days",
    ]

    available = [c for c in fact_cols if c in df.columns]
    subset = df[available].copy()

    for col in ["has_name", "is_mix", "is_live_outcome"]:
        if col in subset.columns:
            subset[col] = subset[col].astype(int)

    for col in ["intake_date", "outcome_date"]:
        if col in subset.columns:
            subset[col] = subset[col].astype(str)

    subset.to_sql("fact_animal", conn, if_exists="replace", index=False)
    print(f"  Loaded {len(subset):,} rows → fact_animal")


def load_agg_monthly_intake(conn: sqlite3.Connection, intakes: pd.DataFrame):
    agg = (
        intakes
        .groupby(["intake_year", "intake_month", "animal_type"], dropna=True)
        .size()
        .reset_index(name="intake_count")
        .rename(columns={"intake_year": "year", "intake_month": "month"})
    )
    agg.to_sql("agg_monthly_intake", conn, if_exists="replace", index=False)
    print(f"  Loaded {len(agg):,} rows → agg_monthly_intake")


def load_agg_outcome_summary(conn: sqlite3.Connection, merged: pd.DataFrame):
    agg = (
        merged
        .groupby(["animal_type", "outcome_category"], dropna=True)
        .agg(
            outcome_count=("animal_id", "count"),
            avg_los_days=("length_of_stay_days", "mean"),
        )
        .reset_index()
    )
    agg["avg_los_days"] = agg["avg_los_days"].round(1)
    agg.to_sql("agg_outcome_summary", conn, if_exists="replace", index=False)
    print(f"  Loaded {len(agg):,} rows → agg_outcome_summary")


def main():
    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    print("Creating schema...")
    create_schema(conn)

    print("Loading data...")
    merged  = pd.read_csv(os.path.join(PROCESSED_DIR, "animals_merged.csv"),  low_memory=False)
    intakes = pd.read_csv(os.path.join(PROCESSED_DIR, "intakes_clean.csv"),   low_memory=False)

    load_fact_animal(conn, merged)
    load_agg_monthly_intake(conn, intakes)
    load_agg_outcome_summary(conn, merged)

    print("\nDatabase verification:")
    for table in ["fact_animal", "agg_monthly_intake", "agg_outcome_summary"]:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count:,} rows")

    conn.close()
    print(f"\nDatabase saved → {DB_PATH}")


if __name__ == "__main__":
    main()
