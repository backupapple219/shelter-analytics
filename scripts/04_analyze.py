"""
04_analyze.py
-------------
Runs all analytical SQL queries against shelter.db.
Exports each result as a CSV to outputs/ for Tableau / Power BI.
Also prints a summary report to console.
"""

import sqlite3
import pandas as pd
import os

DB_PATH    = os.path.join(os.path.dirname(__file__), "..", "data", "shelter.db")
SQL_DIR    = os.path.join(os.path.dirname(__file__), "..", "sql")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")

QUERIES = {
    "monthly_intake_trend": """
        SELECT year, month, animal_type, intake_count
        FROM agg_monthly_intake
        WHERE year >= 2018
        ORDER BY year, month, animal_type
    """,

    "outcome_distribution": """
        SELECT
            animal_type,
            outcome_category,
            outcome_count,
            ROUND(
                100.0 * outcome_count / SUM(outcome_count) OVER (PARTITION BY animal_type),
                1
            ) AS pct_of_type
        FROM agg_outcome_summary
        ORDER BY animal_type, outcome_count DESC
    """,

    "los_by_intake_type": """
        SELECT
            intake_type,
            animal_type,
            COUNT(*) AS animal_count,
            ROUND(AVG(length_of_stay_days), 1) AS avg_los_days
        FROM fact_animal
        WHERE length_of_stay_days BETWEEN 0 AND 730
          AND intake_type IS NOT NULL
        GROUP BY intake_type, animal_type
        HAVING animal_count >= 50
        ORDER BY avg_los_days DESC
    """,

    "top_breeds_by_volume": """
        SELECT primary_breed, animal_type, COUNT(*) AS total_intakes
        FROM fact_animal
        WHERE primary_breed NOT IN ('Unknown', '')
          AND animal_type IN ('Dog', 'Cat')
        GROUP BY primary_breed, animal_type
        ORDER BY total_intakes DESC
        LIMIT 20
    """,

    "dog_breed_adoption_rate": """
        SELECT
            primary_breed,
            COUNT(*) AS total,
            SUM(CASE WHEN outcome_category = 'Adoption' THEN 1 ELSE 0 END) AS adoptions,
            ROUND(
                100.0 * SUM(CASE WHEN outcome_category = 'Adoption' THEN 1 ELSE 0 END) / COUNT(*),
                1
            ) AS adoption_rate_pct
        FROM fact_animal
        WHERE animal_type = 'Dog'
          AND primary_breed NOT IN ('Unknown', '')
        GROUP BY primary_breed
        HAVING total >= 100
        ORDER BY adoption_rate_pct DESC
        LIMIT 15
    """,

    "seasonal_intake": """
        SELECT intake_season, intake_month, animal_type, COUNT(*) AS intake_count
        FROM fact_animal
        WHERE intake_season IS NOT NULL
          AND animal_type IN ('Dog', 'Cat')
        GROUP BY intake_season, intake_month, animal_type
        ORDER BY intake_month
    """,

    "live_outcome_rate_by_year": """
        SELECT
            outcome_year AS year,
            animal_type,
            COUNT(*) AS total_outcomes,
            SUM(is_live_outcome) AS live_outcomes,
            ROUND(100.0 * SUM(is_live_outcome) / COUNT(*), 1) AS live_outcome_rate_pct
        FROM fact_animal
        WHERE outcome_year BETWEEN 2015 AND 2025
          AND animal_type IN ('Dog', 'Cat')
        GROUP BY outcome_year, animal_type
        ORDER BY year, animal_type
    """,

    "named_vs_unnamed": """
        SELECT
            has_name,
            outcome_category,
            COUNT(*) AS count,
            ROUND(AVG(length_of_stay_days), 1) AS avg_los_days
        FROM fact_animal
        WHERE length_of_stay_days BETWEEN 0 AND 365
        GROUP BY has_name, outcome_category
        ORDER BY has_name, count DESC
    """,
}


def run_queries(conn: sqlite3.Connection) -> dict[str, pd.DataFrame]:
    results = {}
    for name, sql in QUERIES.items():
        try:
            df = pd.read_sql_query(sql, conn)
            results[name] = df
            print(f"  ✓ {name}: {len(df)} rows")
        except Exception as e:
            print(f"  ✗ {name}: ERROR — {e}")
    return results


def print_summary(results: dict[str, pd.DataFrame]):
    print("\n" + "=" * 60)
    print("SHELTER ANALYTICS — KEY FINDINGS SUMMARY")
    print("=" * 60)

    # Outcome distribution
    if "outcome_distribution" in results:
        df = results["outcome_distribution"]
        print("\n📊 Outcome Distribution (all species):")
        for _, row in df.iterrows():
            print(f"   {row['animal_type']:<8} | {row['outcome_category']:<20} | "
                  f"{row['outcome_count']:>6,} ({row['pct_of_type']}%)")

    # Live outcome rate trend
    if "live_outcome_rate_by_year" in results:
        df = results["live_outcome_rate_by_year"]
        print("\n📈 Live Outcome Rate by Year (Dogs):")
        dogs = df[df["animal_type"] == "Dog"].tail(6)
        for _, row in dogs.iterrows():
            bar = "█" * int(row["live_outcome_rate_pct"] / 5)
            print(f"   {int(row['year'])} | {bar:<20} {row['live_outcome_rate_pct']}%")

    # Top 5 breeds
    if "top_breeds_by_volume" in results:
        df = results["top_breeds_by_volume"]
        print("\n🐶 Top 5 Dog Breeds by Intake Volume:")
        dogs = df[df["animal_type"] == "Dog"].head(5)
        for i, (_, row) in enumerate(dogs.iterrows(), 1):
            print(f"   {i}. {row['primary_breed']}: {row['total_intakes']:,}")

    # Breed adoption rates
    if "dog_breed_adoption_rate" in results:
        df = results["dog_breed_adoption_rate"]
        print("\n🏆 Top 5 Dog Breeds by Adoption Rate:")
        for i, (_, row) in enumerate(df.head(5).iterrows(), 1):
            print(f"   {i}. {row['primary_breed']}: {row['adoption_rate_pct']}% ({row['total']:,} total)")

    # LOS
    if "los_by_intake_type" in results:
        df = results["los_by_intake_type"]
        print("\n⏱  Average Length of Stay by Intake Type:")
        for _, row in df.head(6).iterrows():
            print(f"   {row['intake_type']:<25} | {row['animal_type']:<5} | "
                  f"{row['avg_los_days']} days")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    print("Running queries...")
    results = run_queries(conn)
    conn.close()

    # Export CSVs
    print("\nExporting CSVs to outputs/...")
    for name, df in results.items():
        path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(path, index=False)
        print(f"  → {name}.csv")

    print_summary(results)
    print("\n✅ Analysis complete. CSVs ready for Tableau / Power BI.")


if __name__ == "__main__":
    main()
