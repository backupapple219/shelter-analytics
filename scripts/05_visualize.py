"""
05_visualize.py
---------------
Generates polished matplotlib/seaborn charts from the outputs/ CSVs.
Charts are saved as PNGs — ready to screenshot into a portfolio or
embed in a README / presentation.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")

# ── Style ─────────────────────────────────────────────────────────────────────
PALETTE = {
    "Dog":              "#4A90D9",
    "Cat":              "#E88B3A",
    "Adoption":         "#2ECC71",
    "Transfer":         "#3498DB",
    "Return to Owner":  "#9B59B6",
    "Euthanasia":       "#E74C3C",
    "Died":             "#95A5A6",
    "Other":            "#BDC3C7",
}

sns.set_theme(style="whitegrid", font_scale=1.05)
plt.rcParams["figure.dpi"] = 150
plt.rcParams["savefig.bbox"] = "tight"
plt.rcParams["font.family"] = "DejaVu Sans"


def load(filename: str) -> pd.DataFrame | None:
    path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(path):
        print(f"  ⚠ Missing: {filename} — skipping chart")
        return None
    return pd.read_csv(path)


def save(fig, name: str):
    path = os.path.join(OUTPUT_DIR, f"{name}.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Saved → {name}.png")


# ── Chart 1: Monthly Intake Trend ─────────────────────────────────────────────
def chart_monthly_intake():
    df = load("monthly_intake_trend.csv")
    if df is None: return

    df = df[df["animal_type"].isin(["Dog", "Cat"])]
    df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

    fig, ax = plt.subplots(figsize=(12, 5))
    for atype, grp in df.groupby("animal_type"):
        ax.plot(grp["date"], grp["intake_count"],
                label=atype, color=PALETTE.get(atype), linewidth=2)

    ax.set_title("Monthly Animal Intake Volume (2018–present)", fontweight="bold", pad=12)
    ax.set_xlabel("Date")
    ax.set_ylabel("Intakes")
    ax.legend(title="Animal Type")
    ax.xaxis.set_major_locator(mticker.MaxNLocator(10))
    plt.xticks(rotation=30)
    save(fig, "chart_01_monthly_intake")


# ── Chart 2: Outcome Distribution ────────────────────────────────────────────
def chart_outcome_distribution():
    df = load("outcome_distribution.csv")
    if df is None: return

    df = df[df["animal_type"].isin(["Dog", "Cat"])]
    pivot = df.pivot_table(index="animal_type", columns="outcome_category",
                           values="outcome_count", fill_value=0)

    # Order columns by total
    pivot = pivot[pivot.sum().sort_values(ascending=False).index]
    colors = [PALETTE.get(c, "#AAA") for c in pivot.columns]

    fig, ax = plt.subplots(figsize=(9, 5))
    pivot.plot(kind="bar", stacked=True, ax=ax, color=colors, edgecolor="white")
    ax.set_title("Outcome Distribution by Animal Type", fontweight="bold", pad=12)
    ax.set_xlabel("")
    ax.set_ylabel("Number of Animals")
    ax.set_xticklabels(pivot.index, rotation=0)
    ax.legend(title="Outcome", bbox_to_anchor=(1.01, 1), loc="upper left")
    save(fig, "chart_02_outcome_distribution")


# ── Chart 3: Live Outcome Rate by Year ───────────────────────────────────────
def chart_live_outcome_rate():
    df = load("live_outcome_rate_by_year.csv")
    if df is None: return

    df = df[df["animal_type"].isin(["Dog", "Cat"])]
    fig, ax = plt.subplots(figsize=(10, 5))

    for atype, grp in df.groupby("animal_type"):
        ax.plot(grp["year"], grp["live_outcome_rate_pct"],
                marker="o", label=atype, color=PALETTE.get(atype), linewidth=2.5)

    ax.set_title("Live Outcome Rate by Year (Dogs & Cats)", fontweight="bold", pad=12)
    ax.set_xlabel("Year")
    ax.set_ylabel("Live Outcome Rate (%)")
    ax.set_ylim(0, 105)
    ax.axhline(90, color="gray", linestyle="--", linewidth=1, alpha=0.6,
               label="No-kill threshold (90%)")
    ax.legend()
    save(fig, "chart_03_live_outcome_rate")


# ── Chart 4: Top 15 Dog Breeds by Intake ─────────────────────────────────────
def chart_top_breeds():
    df = load("top_breeds_by_volume.csv")
    if df is None: return

    dogs = df[df["animal_type"] == "Dog"].head(15).sort_values("total_intakes")
    fig, ax = plt.subplots(figsize=(8, 7))
    bars = ax.barh(dogs["primary_breed"], dogs["total_intakes"],
                   color=PALETTE["Dog"], edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title("Top 15 Dog Breeds by Intake Volume", fontweight="bold", pad=12)
    ax.set_xlabel("Total Intakes")
    ax.set_ylabel("")
    save(fig, "chart_04_top_dog_breeds")


# ── Chart 5: Dog Breed Adoption Rates ────────────────────────────────────────
def chart_breed_adoption_rates():
    df = load("dog_breed_adoption_rate.csv")
    if df is None: return

    df = df.sort_values("adoption_rate_pct", ascending=True)
    colors = ["#E74C3C" if r < 50 else "#2ECC71" for r in df["adoption_rate_pct"]]

    fig, ax = plt.subplots(figsize=(8, 7))
    bars = ax.barh(df["primary_breed"], df["adoption_rate_pct"],
                   color=colors, edgecolor="white")
    ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=9)
    ax.axvline(50, color="gray", linestyle="--", linewidth=1, alpha=0.7)
    ax.set_title("Dog Breed Adoption Rates (min 100 intakes)", fontweight="bold", pad=12)
    ax.set_xlabel("Adoption Rate (%)")
    ax.set_xlim(0, 105)
    save(fig, "chart_05_breed_adoption_rates")


# ── Chart 6: Avg Length of Stay by Intake Type ───────────────────────────────
def chart_los_by_intake_type():
    df = load("los_by_intake_type.csv")
    if df is None: return

    df = df[df["animal_type"].isin(["Dog", "Cat"])]
    pivot = df.pivot_table(index="intake_type", columns="animal_type",
                           values="avg_los_days", fill_value=0)
    pivot = pivot.sort_values("Dog", ascending=True)

    fig, ax = plt.subplots(figsize=(9, 5))
    x = range(len(pivot))
    w = 0.35
    ax.barh([i + w/2 for i in x], pivot.get("Dog", [0]*len(pivot)),
            height=w, label="Dog", color=PALETTE["Dog"])
    ax.barh([i - w/2 for i in x], pivot.get("Cat", [0]*len(pivot)),
            height=w, label="Cat", color=PALETTE["Cat"])
    ax.set_yticks(list(x))
    ax.set_yticklabels(pivot.index)
    ax.set_title("Avg Length of Stay by Intake Type (days)", fontweight="bold", pad=12)
    ax.set_xlabel("Average Days in Shelter")
    ax.legend()
    save(fig, "chart_06_los_by_intake_type")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("Generating charts...")
    chart_monthly_intake()
    chart_outcome_distribution()
    chart_live_outcome_rate()
    chart_top_breeds()
    chart_breed_adoption_rates()
    chart_los_by_intake_type()
    print("\n✅ All charts saved to outputs/")
    print("   Connect outputs/*.csv to Tableau Public or Power BI for the interactive dashboard.")


if __name__ == "__main__":
    main()
