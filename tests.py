import pandas as pd
import numpy as np

df = pd.read_csv("C:\\Users\\backu\\OneDrive\\Desktop\\cs stuff\\AnimalShelterProject\\data\\raw\\outcomes_raw.csv", low_memory=False)
print("Total rows:", len(df))

# Step 1: rename
df = df.rename(columns={"datetime": "outcome_datetime", "monthyear": "outcome_monthyear"})

# Step 2: parse datetime
df["outcome_datetime"] = pd.to_datetime(df["outcome_datetime"], errors="coerce")
if df["outcome_datetime"].dt.tz is not None:
    df["outcome_datetime"] = df["outcome_datetime"].dt.tz_convert(None)

print("Null outcome_datetime after parse:", df["outcome_datetime"].isna().sum())
df = df.dropna(subset=["outcome_datetime"])
print("Rows after dropna:", len(df))

# Step 3: check season_from_month
print("\nUnique months:", sorted(df["outcome_datetime"].dt.month.unique()))
print("Sample dates:", df["outcome_datetime"].head(3).tolist())