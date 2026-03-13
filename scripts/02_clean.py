"""
02_clean.py
-----------
Cleans and normalizes raw intake/outcome CSVs.
Engineers features used for analysis and dashboard.
"""

import pandas as pd
import numpy as np
import os

RAW_DIR       = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def parse_age_to_days(age_str):
    if not isinstance(age_str, str) or age_str.strip() == "":
        return np.nan
    parts = age_str.strip().lower().split()
    if len(parts) < 2:
        return np.nan
    try:
        value = float(parts[0])
    except ValueError:
        return np.nan
    multipliers = {"year": 365, "years": 365, "month": 30, "months": 30,
                   "week": 7, "weeks": 7, "day": 1, "days": 1}
    return value * multipliers.get(parts[1], np.nan)


def normalize_sex(sex_str):
    if not isinstance(sex_str, str) or sex_str.strip() in ("", "Unknown"):
        return "Unknown", "Unknown"
    parts = sex_str.strip().split()
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return "Unknown", parts[0]
    return "Unknown", "Unknown"


def extract_primary_breed(breed_str):
    if not isinstance(breed_str, str):
        return "Unknown"
    breed = breed_str.split("/")[0].replace(" Mix", "").strip()
    return breed if breed else "Unknown"


def season_from_month(month):
    return {12: "Winter", 1: "Winter", 2: "Winter",
             3: "Spring", 4: "Spring", 5: "Spring",
             6: "Summer", 7: "Summer", 8: "Summer",
             9: "Fall", 10: "Fall", 11: "Fall"}[month]


def clean_intakes(df):
    print("  Cleaning intakes...")
    df = df.copy()
    df = df.rename(columns={"datetime": "intake_datetime", "monthyear": "intake_monthyear"})

    df["intake_datetime"] = pd.to_datetime(df["intake_datetime"], errors="coerce", format="mixed", utc=True)
    df["intake_datetime"] = df["intake_datetime"].dt.tz_convert(None)
    df = df.dropna(subset=["intake_datetime"])

    df["intake_date"]   = df["intake_datetime"].dt.date
    df["intake_year"]   = df["intake_datetime"].dt.year
    df["intake_month"]  = df["intake_datetime"].dt.month
    df["intake_dow"]    = df["intake_datetime"].dt.day_name()
    df["intake_season"] = df["intake_month"].map(season_from_month)

    df[["neuter_status_intake", "sex"]] = df["sex_upon_intake"].apply(
        lambda x: pd.Series(normalize_sex(x))
    )

    df["age_days_intake"]  = df["age_upon_intake"].apply(parse_age_to_days)
    df["age_years_intake"] = (df["age_days_intake"] / 365).round(1)

    def age_bucket(days):
        if pd.isna(days):    return "Unknown"
        if days < 90:        return "< 3 months"
        if days < 365:       return "3-12 months"
        if days < 365 * 3:   return "1-3 years"
        if days < 365 * 7:   return "3-7 years"
        return "7+ years"

    df["age_group"]     = df["age_days_intake"].apply(age_bucket)
    df["primary_breed"] = df["breed"].apply(extract_primary_breed)
    df["is_mix"]        = df["breed"].str.contains("Mix|/", na=False)

    for col in ["intake_type", "intake_condition", "animal_type"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    df["name"]     = df["name"].fillna("").str.strip().str.title()
    df["has_name"] = df["name"].ne("")

    return df


def clean_outcomes(df):
    print("  Cleaning outcomes...")
    df = df.copy()
    df = df.rename(columns={"datetime": "outcome_datetime", "monthyear": "outcome_monthyear"})

    df["outcome_datetime"] = pd.to_datetime(df["outcome_datetime"], errors="coerce", format="mixed", utc=True)
    df["outcome_datetime"] = df["outcome_datetime"].dt.tz_convert(None)
    df = df.dropna(subset=["outcome_datetime"])

    df["outcome_date"]   = df["outcome_datetime"].dt.date
    df["outcome_year"]   = df["outcome_datetime"].dt.year
    df["outcome_month"]  = df["outcome_datetime"].dt.month
    df["outcome_dow"]    = df["outcome_datetime"].dt.day_name()
    df["outcome_season"] = df["outcome_month"].map(season_from_month)

    df[["neuter_status_outcome", "sex_outcome"]] = df["sex_upon_outcome"].apply(
        lambda x: pd.Series(normalize_sex(x))
    )

    def categorize_outcome(ot):
        if not isinstance(ot, str): return "Other"
        ot = ot.strip().lower()
        if "adopt"    in ot: return "Adoption"
        if "transfer" in ot: return "Transfer"
        if "return"   in ot: return "Return to Owner"
        if "euthan"   in ot: return "Euthanasia"
        if "died"     in ot: return "Died"
        if "rto"      in ot: return "Return to Owner"
        return "Other"

    df["outcome_category"] = df["outcome_type"].apply(categorize_outcome)
    df["is_live_outcome"]  = df["outcome_category"].isin(["Adoption", "Transfer", "Return to Owner"])
    df["primary_breed"]    = df["breed"].apply(extract_primary_breed)

    for col in ["outcome_type", "outcome_subtype", "animal_type"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()

    return df


def merge_intake_outcome(intakes, outcomes):
    print("  Merging intakes + outcomes...")

    intake_latest = (
        intakes.sort_values("intake_datetime")
               .drop_duplicates("animal_id", keep="last")
    )
    outcome_latest = (
        outcomes.sort_values("outcome_datetime")
                .drop_duplicates("animal_id", keep="last")
    )

    outcome_cols = ["animal_id", "outcome_datetime", "outcome_date",
                    "outcome_year", "outcome_month", "outcome_season",
                    "outcome_dow", "outcome_type", "outcome_category",
                    "is_live_outcome", "neuter_status_outcome"]
    if "outcome_subtype" in outcome_latest.columns:
        outcome_cols.append("outcome_subtype")

    merged = intake_latest.merge(outcome_latest[outcome_cols], on="animal_id", how="inner")

    merged["length_of_stay_days"] = (
        merged["outcome_datetime"] - merged["intake_datetime"]
    ).dt.days

    merged = merged[merged["length_of_stay_days"] >= 0]
    return merged


def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    intakes_raw  = pd.read_csv(os.path.join(RAW_DIR, "intakes_raw.csv"),  low_memory=False)
    outcomes_raw = pd.read_csv(os.path.join(RAW_DIR, "outcomes_raw.csv"), low_memory=False)
    print(f"Raw intakes:  {len(intakes_raw):,} rows")
    print(f"Raw outcomes: {len(outcomes_raw):,} rows")

    intakes  = clean_intakes(intakes_raw)
    outcomes = clean_outcomes(outcomes_raw)

    intakes.to_csv(os.path.join(PROCESSED_DIR, "intakes_clean.csv"),  index=False)
    outcomes.to_csv(os.path.join(PROCESSED_DIR, "outcomes_clean.csv"), index=False)
    print(f"  Saved intakes_clean.csv  ({len(intakes):,} rows)")
    print(f"  Saved outcomes_clean.csv ({len(outcomes):,} rows)")

    merged = merge_intake_outcome(intakes, outcomes)
    merged.to_csv(os.path.join(PROCESSED_DIR, "animals_merged.csv"), index=False)
    print(f"  Saved animals_merged.csv ({len(merged):,} rows)")

    print("\nCleaning complete.")


if __name__ == "__main__":
    main()